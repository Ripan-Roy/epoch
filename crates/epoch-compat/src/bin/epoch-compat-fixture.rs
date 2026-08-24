use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use epoch_compat::{
    amqp::{AmqpConfig, AmqpServer},
    kafka::{KafkaConfig, KafkaServer},
    redis::{RedisConfig, RedisServer},
    test_support::MemoryBackend,
};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    redis_listen: SocketAddr,
    #[arg(long)]
    kafka_listen: SocketAddr,
    #[arg(long)]
    amqp_listen: SocketAddr,
    #[arg(long)]
    kafka_advertised_host: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize fixture logging: {error}"))?;
    let args = Args::parse();
    let backend = Arc::new(MemoryBackend::with_resources(
        "sessions", "events", 3, "jobs",
    ));
    let redis = RedisServer::new(
        Arc::clone(&backend),
        RedisConfig {
            cache: "sessions".into(),
            password: Some("compat-secret".into()),
            max_connections: 64,
        },
    )?;
    let kafka = KafkaServer::new(
        Arc::clone(&backend),
        KafkaConfig {
            advertised_host: args.kafka_advertised_host,
            port: args.kafka_listen.port(),
            node_id: 1,
            max_connections: 64,
        },
    )?;
    let amqp = AmqpServer::new(
        backend,
        AmqpConfig {
            username: "epoch".into(),
            password: "compat-secret".into(),
            max_connections: 64,
            heartbeat_seconds: 10,
        },
    )?;
    let redis_listener = TcpListener::bind(args.redis_listen)
        .await
        .context("bind Redis fixture")?;
    let kafka_listener = TcpListener::bind(args.kafka_listen)
        .await
        .context("bind Kafka fixture")?;
    let amqp_listener = TcpListener::bind(args.amqp_listen)
        .await
        .context("bind AMQP fixture")?;
    println!(
        "READY redis={} kafka={} amqp={}",
        args.redis_listen, args.kafka_listen, args.amqp_listen
    );
    tokio::select! {
        result = redis.serve(redis_listener) => result.context("Redis fixture failed")?,
        result = kafka.serve(kafka_listener) => result.context("Kafka fixture failed")?,
        result = amqp.serve(amqp_listener) => result.context("AMQP fixture failed")?,
        signal = tokio::signal::ctrl_c() => signal.context("fixture shutdown signal failed")?,
    }
    Ok(())
}
