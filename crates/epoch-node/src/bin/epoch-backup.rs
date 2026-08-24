//! Managed regional backup capture, authenticated encryption, retention, and restore utility.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::http::StatusCode;
use clap::{Args, Parser, Subcommand};
use epoch_node::{
    regional_backup_api::{REGIONAL_BACKUP_PATH, RegionalBackupArtifact},
    regional_backup_encryption::{
        decrypt_backup, encrypt_backup, enforce_encrypted_retention, inspect_encrypted_backup,
        load_encryption_key, load_retention_keyring,
    },
    transport_security::{ClientTlsFiles, configure_client_builder},
};
use serde::Serialize;
use url::Url;

const MAX_REGIONAL_BACKUP_BYTES: usize = 128 * 1024 * 1024;
const MAX_TOKEN_BYTES: u64 = 16 * 1024;
const MAX_ENDPOINTS: usize = 1_024;

#[derive(Debug, Parser)]
#[command(
    name = "epoch-backup",
    version,
    about = "Capture, encrypt, retain, inspect, and decrypt Epoch regional backups"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Capture(CaptureArgs),
    Decrypt {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        encryption_key: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Inspect {
        #[arg(long)]
        input: PathBuf,
    },
}

#[derive(Debug, Args)]
struct CaptureArgs {
    #[arg(long, env = "EPOCH_BACKUP_ENDPOINTS", value_delimiter = ',')]
    endpoints: Vec<Url>,
    #[arg(long, env = "EPOCH_BACKUP_TOKEN_PATH")]
    token_path: PathBuf,
    #[arg(long, env = "EPOCH_BACKUP_TLS_CA_PATH")]
    tls_ca: PathBuf,
    #[arg(long, env = "EPOCH_BACKUP_TLS_CERT_PATH")]
    tls_certificate: PathBuf,
    #[arg(long, env = "EPOCH_BACKUP_TLS_KEY_PATH")]
    tls_private_key: PathBuf,
    #[arg(long, env = "EPOCH_BACKUP_ENCRYPTION_KEY_PATH")]
    encryption_key: PathBuf,
    #[arg(long, env = "EPOCH_BACKUP_KEY_ID")]
    key_id: String,
    #[arg(long, env = "EPOCH_BACKUP_OUTPUT_DIR")]
    output_dir: PathBuf,
    #[arg(long, env = "EPOCH_BACKUP_RETENTION_COUNT", default_value_t = 7)]
    retention_count: usize,
    #[arg(long, env = "EPOCH_BACKUP_CAPTURE_ROUNDS", default_value_t = 20)]
    capture_rounds: usize,
    #[arg(long, env = "EPOCH_BACKUP_STATUS_PATH")]
    status_path: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct CaptureStatus {
    state: &'static str,
    object_name: String,
    captured_at_ms: u64,
    manifest_sha256: String,
    plaintext_sha256: String,
    key_id: String,
    retained_objects: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let result = match Cli::parse().command {
        Command::Capture(args) => capture(args).await?,
        Command::Decrypt {
            input,
            encryption_key,
            output,
        } => {
            let key = load_encryption_key(&encryption_key)?;
            let artifact = decrypt_backup(&input, &key, &output)?;
            serde_json::to_value(serde_json::json!({
                "state": "decrypted",
                "output": output,
                "captured_at_ms": artifact.captured_at_ms,
                "manifest_sha256": artifact.manifest_sha256,
            }))?
        }
        Command::Inspect { input } => serde_json::to_value(inspect_encrypted_backup(&input)?)?,
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn capture(args: CaptureArgs) -> Result<serde_json::Value, Box<dyn Error>> {
    validate_capture_args(&args)?;
    let token = read_token(&args.token_path)?;
    let client = configure_client_builder(
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_mins(1)),
        &ClientTlsFiles {
            ca: args.tls_ca,
            certificate: args.tls_certificate,
            private_key: args.tls_private_key,
        },
    )?
    .build()?;
    let artifact =
        capture_from_cluster(&client, &args.endpoints, &token, args.capture_rounds).await?;
    let keyring = load_retention_keyring(&args.encryption_key, &args.key_id)?;
    let key = keyring
        .get(&args.key_id)
        .expect("validated retention keyring always contains the active key");
    let object_name = format!(
        "{}-{}.epoch-backup.enc",
        artifact.captured_at_ms, artifact.manifest_sha256
    );
    let output = args.output_dir.join(&object_name);
    let encrypted = encrypt_backup(&artifact, key, &args.key_id, now_ms()?, &output)?;
    let retained_objects =
        enforce_encrypted_retention(&args.output_dir, args.retention_count, &keyring)?;
    let status = CaptureStatus {
        state: "succeeded",
        object_name,
        captured_at_ms: artifact.captured_at_ms,
        manifest_sha256: artifact.manifest_sha256,
        plaintext_sha256: encrypted.plaintext_sha256,
        key_id: encrypted.key_id,
        retained_objects,
    };
    let encoded = serde_json::to_vec(&status)?;
    if let Some(path) = args.status_path {
        fs::write(path, &encoded)?;
    }
    Ok(serde_json::from_slice(&encoded)?)
}

fn validate_capture_args(args: &CaptureArgs) -> Result<(), Box<dyn Error>> {
    if args.endpoints.is_empty() || args.endpoints.len() > MAX_ENDPOINTS {
        return Err(format!("capture requires 1..={MAX_ENDPOINTS} endpoints").into());
    }
    for endpoint in &args.endpoints {
        if endpoint.scheme() != "https"
            || endpoint.cannot_be_a_base()
            || endpoint.host_str().is_none()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !matches!(endpoint.path(), "" | "/")
        {
            return Err(
                format!("backup endpoint must be an HTTPS authority URL: {endpoint}").into(),
            );
        }
    }
    if args.retention_count == 0 || args.retention_count > 10_000 {
        return Err("retention count must be between 1 and 10000".into());
    }
    if args.capture_rounds == 0 || args.capture_rounds > 1_000 {
        return Err("capture rounds must be between 1 and 1000".into());
    }
    if !args.output_dir.is_dir() {
        return Err(format!(
            "backup output directory {} does not exist",
            args.output_dir.display()
        )
        .into());
    }
    Ok(())
}

async fn capture_from_cluster(
    client: &reqwest::Client,
    endpoints: &[Url],
    token: &str,
    rounds: usize,
) -> Result<RegionalBackupArtifact, Box<dyn Error>> {
    let mut last_errors = Vec::new();
    for round in 1..=rounds {
        last_errors.clear();
        for endpoint in endpoints {
            let backup_endpoint = endpoint.join(REGIONAL_BACKUP_PATH.trim_start_matches('/'))?;
            match client
                .post(backup_endpoint.clone())
                .bearer_auth(token)
                .send()
                .await
            {
                Ok(response) if response.status() == StatusCode::CREATED => {
                    let encoded = bounded_response(response).await?;
                    return Ok(RegionalBackupArtifact::decode(&encoded)?);
                }
                Ok(response) => {
                    let status = response.status();
                    let body = bounded_response(response).await.unwrap_or_default();
                    last_errors.push(format!(
                        "{backup_endpoint} returned {status}: {}",
                        String::from_utf8_lossy(&body)
                    ));
                }
                Err(error) => last_errors.push(format!("{backup_endpoint}: {error}")),
            }
        }
        if round < rounds {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    Err(format!(
        "no catalog leader produced a backup after {rounds} rounds: {}",
        last_errors.join("; ")
    )
    .into())
}

async fn bounded_response(mut response: reqwest::Response) -> Result<Vec<u8>, Box<dyn Error>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_REGIONAL_BACKUP_BYTES as u64)
    {
        return Err("regional backup response exceeds the size limit".into());
    }
    let mut encoded = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if encoded.len().saturating_add(chunk.len()) > MAX_REGIONAL_BACKUP_BYTES {
            return Err("regional backup response exceeds the size limit".into());
        }
        encoded.extend_from_slice(&chunk);
    }
    Ok(encoded)
}

fn read_token(path: &Path) -> Result<String, Box<dyn Error>> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_TOKEN_BYTES {
        return Err("backup bearer token must be a bounded non-empty regular file".into());
    }
    let token = fs::read_to_string(path)?;
    let token = token.trim_end_matches(['\r', '\n']);
    if token.is_empty()
        || token
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err("backup bearer token contains invalid whitespace or controls".into());
    }
    Ok(token.into())
}

fn now_ms() -> Result<u64, Box<dyn Error>> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::*;

    #[test]
    fn capture_requires_every_security_and_destination_input() {
        assert!(Cli::try_parse_from(["epoch-backup", "capture"]).is_err());
        assert!(
            Cli::try_parse_from([
                "epoch-backup",
                "decrypt",
                "--input",
                "backup.enc",
                "--encryption-key",
                "key",
                "--output",
                "backup.json",
            ])
            .is_ok()
        );
    }
}
