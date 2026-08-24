use std::{
    fs::File,
    io::{self, BufReader, Read as _},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use epoch_compat::{
    MAX_FRAME_BYTES, NativeHttpBackend, NativeHttpConfig,
    amqp::{AmqpConfig, AmqpServer},
    kafka::{KafkaConfig, KafkaServer},
    redis::{RedisConfig, RedisServer},
    scanner::{Protocol, SupportLevel, scan},
};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Parser)]
#[command(
    name = "epoch-compat",
    version,
    about = "Run bounded Redis, Kafka, and RabbitMQ protocol gateways for Epoch"
)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(
        long,
        env = "EPOCH_COMPAT_ENDPOINTS",
        value_delimiter = ',',
        default_value = "http://127.0.0.1:7601"
    )]
    endpoints: Vec<Url>,
    #[arg(long, env = "EPOCH_COMPAT_TOKEN", hide_env_values = true)]
    token: Option<String>,
    #[arg(long, env = "EPOCH_COMPAT_ORGANIZATION", default_value = "acme")]
    organization: String,
    #[arg(long, env = "EPOCH_COMPAT_PROJECT", default_value = "shop")]
    project: String,
    #[arg(long, env = "EPOCH_COMPAT_ENVIRONMENT", default_value = "dev")]
    environment: String,
    #[arg(long, env = "EPOCH_COMPAT_NAMESPACE", default_value = "core")]
    namespace: String,
    #[arg(long, env = "EPOCH_COMPAT_BACKEND_TIMEOUT_MS", default_value_t = 5_000)]
    backend_timeout_ms: u64,
    #[arg(long, env = "EPOCH_COMPAT_MAX_CONNECTIONS", default_value_t = 1_024)]
    max_connections: usize,

    #[arg(
        long,
        env = "EPOCH_COMPAT_REDIS_LISTEN",
        default_value = "127.0.0.1:6379"
    )]
    redis_listen: SocketAddr,
    #[arg(long, env = "EPOCH_COMPAT_REDIS_CACHE", default_value = "sessions")]
    redis_cache: String,
    #[arg(long, env = "EPOCH_COMPAT_REDIS_PASSWORD", hide_env_values = true)]
    redis_password: Option<String>,

    #[arg(
        long,
        env = "EPOCH_COMPAT_KAFKA_LISTEN",
        default_value = "127.0.0.1:9092"
    )]
    kafka_listen: SocketAddr,
    #[arg(
        long,
        env = "EPOCH_COMPAT_KAFKA_ADVERTISED_HOST",
        default_value = "127.0.0.1"
    )]
    kafka_advertised_host: String,
    #[arg(
        long,
        env = "EPOCH_COMPAT_KAFKA_ADVERTISED_PORT",
        default_value_t = 9_092
    )]
    kafka_advertised_port: u16,
    #[arg(long, env = "EPOCH_COMPAT_KAFKA_NODE_ID", default_value_t = 1)]
    kafka_node_id: i32,

    #[arg(
        long,
        env = "EPOCH_COMPAT_AMQP_LISTEN",
        default_value = "127.0.0.1:5672"
    )]
    amqp_listen: SocketAddr,
    #[arg(long, env = "EPOCH_COMPAT_AMQP_USERNAME", default_value = "epoch")]
    amqp_username: String,
    #[arg(long, env = "EPOCH_COMPAT_AMQP_PASSWORD", hide_env_values = true)]
    amqp_password: Option<String>,
    #[arg(
        long,
        env = "EPOCH_COMPAT_AMQP_HEARTBEAT_SECONDS",
        default_value_t = 30
    )]
    amqp_heartbeat_seconds: u16,
    #[arg(long, env = "EPOCH_LOG", default_value = "info")]
    log: String,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Assess an observed protocol-usage manifest before migration.
    Scan(ScanArgs),
}

#[derive(Debug, ClapArgs)]
struct ScanArgs {
    /// Manifest protocol, or auto when each line starts with a protocol name.
    #[arg(long, value_enum, default_value_t = ScannerProtocol::Auto)]
    protocol: ScannerProtocol,
    /// Output format for the versioned assessment report.
    #[arg(long, value_enum, default_value_t = ReportFormat::Text)]
    format: ReportFormat,
    /// Return a non-zero status when this level or a less compatible level is found.
    #[arg(long, value_enum, default_value_t = FailureThreshold::Unsupported)]
    fail_on: FailureThreshold,
    /// Newline-delimited usage manifest, or - for standard input.
    input: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ScannerProtocol {
    Auto,
    Redis,
    Kafka,
    Amqp091,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReportFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum FailureThreshold {
    Partial,
    Unknown,
    Unsupported,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(Command::Scan(scan_args)) = &args.command {
        return run_scan(scan_args);
    }
    let (token, amqp_password) = runtime_credentials(&args)?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&args.log))
        .init();

    let backend = Arc::new(NativeHttpBackend::new(NativeHttpConfig {
        endpoints: args.endpoints,
        token,
        organization: args.organization,
        project: args.project,
        environment: args.environment,
        namespace: args.namespace,
        timeout: Duration::from_millis(args.backend_timeout_ms),
    })?);
    let redis_cache = args.redis_cache;
    let redis = RedisServer::new(
        Arc::clone(&backend),
        RedisConfig {
            cache: redis_cache.clone(),
            password: args.redis_password,
            max_connections: args.max_connections,
        },
    )?;
    let kafka = KafkaServer::new(
        Arc::clone(&backend),
        KafkaConfig {
            advertised_host: args.kafka_advertised_host,
            port: args.kafka_advertised_port,
            node_id: args.kafka_node_id,
            max_connections: args.max_connections,
        },
    )?;
    let amqp = AmqpServer::new(
        backend,
        AmqpConfig {
            username: args.amqp_username,
            password: amqp_password,
            max_connections: args.max_connections,
            heartbeat_seconds: args.amqp_heartbeat_seconds,
        },
    )?;

    let redis_listener = TcpListener::bind(args.redis_listen)
        .await
        .with_context(|| format!("failed to bind Redis listener at {}", args.redis_listen))?;
    let kafka_listener = TcpListener::bind(args.kafka_listen)
        .await
        .with_context(|| format!("failed to bind Kafka listener at {}", args.kafka_listen))?;
    let amqp_listener = TcpListener::bind(args.amqp_listen)
        .await
        .with_context(|| format!("failed to bind AMQP listener at {}", args.amqp_listen))?;

    info!(listen = %args.redis_listen, cache = %redis_cache, "Redis compatibility listener ready");
    info!(listen = %args.kafka_listen, "Kafka compatibility listener ready");
    info!(listen = %args.amqp_listen, "AMQP 0-9-1 compatibility listener ready");

    tokio::select! {
        result = redis.serve(redis_listener) => result.context("Redis listener failed")?,
        result = kafka.serve(kafka_listener) => result.context("Kafka listener failed")?,
        result = amqp.serve(amqp_listener) => result.context("AMQP listener failed")?,
        signal = tokio::signal::ctrl_c() => signal.context("failed to install shutdown signal")?,
    }
    Ok(())
}

fn runtime_credentials(args: &Args) -> Result<(String, String)> {
    let token = args
        .token
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("--token or EPOCH_COMPAT_TOKEN is required to run the gateway")?;
    let amqp_password = args
        .amqp_password
        .as_deref()
        .filter(|value| !value.is_empty())
        .context("--amqp-password or EPOCH_COMPAT_AMQP_PASSWORD is required to run the gateway")?;
    Ok((token.to_owned(), amqp_password.to_owned()))
}

fn run_scan(args: &ScanArgs) -> Result<()> {
    let protocol = match args.protocol {
        ScannerProtocol::Auto => None,
        ScannerProtocol::Redis => Some(Protocol::Redis),
        ScannerProtocol::Kafka => Some(Protocol::Kafka),
        ScannerProtocol::Amqp091 => Some(Protocol::Amqp091),
    };
    let mut input = Vec::new();
    if args.input.as_os_str() == "-" {
        io::stdin()
            .lock()
            .take(u64::try_from(MAX_FRAME_BYTES + 1).unwrap_or(u64::MAX))
            .read_to_end(&mut input)
            .context("failed to read compatibility usage from stdin")?;
    } else {
        let file = File::open(&args.input)
            .with_context(|| format!("failed to open {}", args.input.display()))?;
        file.take(u64::try_from(MAX_FRAME_BYTES + 1).unwrap_or(u64::MAX))
            .read_to_end(&mut input)
            .with_context(|| format!("failed to read {}", args.input.display()))?;
    }
    let report = scan(BufReader::new(input.as_slice()), protocol)?;
    match args.format {
        ReportFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        ReportFormat::Text => {
            for assessment in &report.assessments {
                println!(
                    "{:?}\t{:?}\t{}\t{}",
                    assessment.protocol,
                    assessment.level,
                    assessment
                        .feature
                        .replace(|character: char| character.is_control(), "?"),
                    assessment.detail
                );
            }
            println!(
                "supported={} partial={} unknown={} unsupported={}",
                report.supported, report.partial, report.unknown, report.unsupported
            );
        }
    }
    let threshold = match args.fail_on {
        FailureThreshold::Partial => SupportLevel::Partial,
        FailureThreshold::Unknown => SupportLevel::Unknown,
        FailureThreshold::Unsupported => SupportLevel::Unsupported,
    };
    if report.fails_at(threshold) {
        bail!(
            "compatibility scan reached the {:?} failure threshold",
            args.fail_on
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory as _;

    #[test]
    fn gateway_credentials_are_required_and_never_have_embedded_defaults() {
        let args = Args::try_parse_from(["epoch-compat"]).unwrap();
        let error = runtime_credentials(&args).unwrap_err();
        assert!(error.to_string().contains("EPOCH_COMPAT_TOKEN"));
    }

    #[test]
    fn gateway_secret_environment_values_are_hidden_from_help() {
        let command = Args::command();
        for id in ["token", "redis_password", "amqp_password"] {
            let argument = command
                .get_arguments()
                .find(|argument| argument.get_id() == id)
                .unwrap_or_else(|| panic!("missing secret argument {id}"));
            assert!(
                argument.is_hide_env_values_set(),
                "{id} must redact its environment value from CLI help"
            );
        }
    }

    #[test]
    fn scanner_remains_usable_without_gateway_credentials() {
        let args = Args::try_parse_from(["epoch-compat", "scan", "-"]).unwrap();
        assert!(matches!(args.command, Some(Command::Scan(_))));
        assert!(args.token.is_none());
        assert!(args.amqp_password.is_none());
    }
}
