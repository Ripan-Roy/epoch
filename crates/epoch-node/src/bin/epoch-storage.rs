use std::{path::PathBuf, process::ExitCode, time::SystemTime};

use clap::{Parser, Subcommand};
use epoch_node::regional_backup::{
    create_backup, create_backup_set, inspect_backup, inspect_backup_set, restore_backup,
    restore_backup_set_node,
};

#[derive(Debug, Parser)]
#[command(
    name = "epoch-storage",
    version,
    about = "Create, verify, and restore Epoch node-volume backups"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Backup {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        node_id: Option<u64>,
    },
    Inspect {
        #[arg(long)]
        input: PathBuf,
    },
    Restore {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        data_dir: PathBuf,
    },
    SetCreate {
        #[arg(long = "node-artifact", required = true)]
        node_artifacts: Vec<PathBuf>,
        #[arg(long)]
        output: PathBuf,
    },
    SetInspect {
        #[arg(long)]
        input: PathBuf,
    },
    SetRestore {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        node_id: u64,
        #[arg(long)]
        data_dir: PathBuf,
    },
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("epoch-storage: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = match cli.command {
        Command::Backup {
            data_dir,
            output,
            node_id,
        } => serde_json::to_value(create_backup(&data_dir, &output, now_ms()?, node_id)?)?,
        Command::Inspect { input } => serde_json::to_value(inspect_backup(&input)?)?,
        Command::Restore { input, data_dir } => {
            serde_json::to_value(restore_backup(&input, &data_dir)?)?
        }
        Command::SetCreate {
            node_artifacts,
            output,
        } => serde_json::to_value(create_backup_set(&node_artifacts, &output)?)?,
        Command::SetInspect { input } => serde_json::to_value(inspect_backup_set(&input)?)?,
        Command::SetRestore {
            manifest,
            node_id,
            data_dir,
        } => serde_json::to_value(restore_backup_set_node(&manifest, node_id, &data_dir)?)?,
    };
    println!("{}", serde_json::to_string_pretty(&metadata)?);
    Ok(())
}

fn now_ms() -> Result<u64, Box<dyn std::error::Error>> {
    Ok(SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn backup_and_restore_commands_require_explicit_paths() {
        assert!(Cli::try_parse_from(["epoch-storage", "backup"]).is_err());
        assert!(Cli::try_parse_from(["epoch-storage", "restore"]).is_err());
        assert!(
            Cli::try_parse_from([
                "epoch-storage",
                "backup",
                "--data-dir",
                "data",
                "--output",
                "backup.json",
            ])
            .is_ok()
        );
    }
}
