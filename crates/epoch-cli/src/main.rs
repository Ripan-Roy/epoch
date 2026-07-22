use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use epoch_core::EventEnvelope;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};

#[derive(Debug, Parser)]
#[command(name = "epoch", version, about = "Manage and use an Epoch node")]
struct Cli {
    #[arg(long, env = "EPOCH_URL", default_value = "http://127.0.0.1:7601")]
    url: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Health,
    Explain,
    Cache(CacheArgs),
    Stream(StreamArgs),
    Queue(QueueArgs),
    Bus(BusArgs),
}

#[derive(Debug, Args)]
struct CacheArgs {
    #[command(subcommand)]
    command: CacheCommand,
}

#[derive(Debug, Subcommand)]
enum CacheCommand {
    Create {
        name: String,
        #[arg(long, default_value_t = 10_000)]
        max_entries: usize,
        #[arg(long)]
        default_ttl_ms: Option<u64>,
        #[arg(long, default_value = "no_eviction")]
        eviction: String,
    },
    Set {
        cache: String,
        key: String,
        value: String,
        #[arg(long)]
        ttl_ms: Option<u64>,
        #[arg(long)]
        expected_version: Option<u64>,
    },
    Get {
        cache: String,
        key: String,
    },
    Delete {
        cache: String,
        key: String,
    },
    Increment {
        cache: String,
        key: String,
        #[arg(default_value_t = 1)]
        delta: i64,
    },
}

#[derive(Debug, Args)]
struct StreamArgs {
    #[command(subcommand)]
    command: StreamCommand,
}

#[derive(Debug, Subcommand)]
enum StreamCommand {
    Create {
        name: String,
        #[arg(long, default_value_t = 1)]
        partitions: u32,
    },
    Append(EventArgsWithResource),
    Fetch {
        stream: String,
        #[arg(long, default_value_t = 0)]
        partition: u32,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
}

#[derive(Debug, Args)]
struct QueueArgs {
    #[command(subcommand)]
    command: QueueCommand,
}

#[derive(Debug, Subcommand)]
enum QueueCommand {
    Create {
        name: String,
        #[arg(long, default_value_t = 30_000)]
        visibility_timeout_ms: u64,
    },
    Send(EventArgsWithResource),
    Receive {
        queue: String,
        #[arg(long)]
        consumer: String,
        #[arg(long, default_value_t = 1)]
        max_messages: usize,
        #[arg(long)]
        visibility_timeout_ms: Option<u64>,
    },
    Ack {
        queue: String,
        token: String,
    },
    Release {
        queue: String,
        token: String,
        #[arg(long, default_value_t = 0)]
        delay_ms: u64,
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Debug, Args)]
struct BusArgs {
    #[command(subcommand)]
    command: BusCommand,
}

#[derive(Debug, Subcommand)]
enum BusCommand {
    Create {
        name: String,
    },
    Publish(EventArgsWithResource),
    Replay {
        bus: String,
        #[arg(long, default_value_t = 0)]
        from_ms: u64,
        #[arg(long, default_value_t = u64::MAX)]
        to_ms: u64,
        #[arg(long)]
        event_type: Option<String>,
    },
}

#[derive(Debug, Args)]
struct EventArgsWithResource {
    resource: String,
    #[arg(long)]
    source: String,
    #[arg(long = "type")]
    event_type: String,
    #[arg(long, default_value = "{}")]
    payload: String,
    #[arg(long)]
    key: Option<String>,
    #[arg(long)]
    dedupe_id: Option<String>,
    #[arg(long)]
    id: Option<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("epoch: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let base = cli.url.trim_end_matches('/');
    let value = match cli.command {
        Command::Health => request(&client, Method::GET, &format!("{base}/healthz"), None).await?,
        Command::Explain => {
            let health = request(&client, Method::GET, &format!("{base}/healthz"), None).await?;
            let resources =
                request(&client, Method::GET, &format!("{base}/v1/resources"), None).await?;
            json!({
                "health": health,
                "resources": resources,
                "notice": "Configured and achieved guarantees are distinct; standalone mode cannot provide multi-node quorum."
            })
        }
        Command::Cache(args) => run_cache(&client, base, args.command).await?,
        Command::Stream(args) => run_stream(&client, base, args.command).await?,
        Command::Queue(args) => run_queue(&client, base, args.command).await?,
        Command::Bus(args) => run_bus(&client, base, args.command).await?,
    };
    if value != Value::Null {
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    Ok(())
}

async fn run_cache(
    client: &Client,
    base: &str,
    command: CacheCommand,
) -> Result<Value, Box<dyn std::error::Error>> {
    match command {
        CacheCommand::Create {
            name,
            max_entries,
            default_ttl_ms,
            eviction,
        } => {
            request(
                client,
                Method::POST,
                &format!("{base}/v1/caches/{name}"),
                Some(json!({
                    "max_entries": max_entries,
                    "default_ttl_ms": default_ttl_ms,
                    "eviction": eviction,
                    "durability": "volatile"
                })),
            )
            .await
        }
        CacheCommand::Set {
            cache,
            key,
            value,
            ttl_ms,
            expected_version,
        } => {
            request(
                client,
                Method::PUT,
                &format!("{base}/v1/caches/{cache}/keys/{key}"),
                Some(json!({
                    "value": {"kind": "string", "value": value},
                    "ttl_ms": ttl_ms,
                    "expected_version": expected_version
                })),
            )
            .await
        }
        CacheCommand::Get { cache, key } => {
            request(
                client,
                Method::GET,
                &format!("{base}/v1/caches/{cache}/keys/{key}"),
                None,
            )
            .await
        }
        CacheCommand::Delete { cache, key } => {
            request(
                client,
                Method::DELETE,
                &format!("{base}/v1/caches/{cache}/keys/{key}"),
                None,
            )
            .await
        }
        CacheCommand::Increment { cache, key, delta } => {
            request(
                client,
                Method::POST,
                &format!("{base}/v1/caches/{cache}/keys/{key}/increment"),
                Some(json!({"delta": delta})),
            )
            .await
        }
    }
}

async fn run_stream(
    client: &Client,
    base: &str,
    command: StreamCommand,
) -> Result<Value, Box<dyn std::error::Error>> {
    match command {
        StreamCommand::Create { name, partitions } => {
            request(
                client,
                Method::POST,
                &format!("{base}/v1/streams/{name}"),
                Some(json!({
                    "partitions": partitions,
                    "durability": "local_durable",
                    "max_records_per_partition": null
                })),
            )
            .await
        }
        StreamCommand::Append(args) => {
            let (resource, envelope) = make_event(args)?;
            request(
                client,
                Method::POST,
                &format!("{base}/v1/streams/{resource}/records"),
                Some(json!({"envelope": envelope, "partition": null})),
            )
            .await
        }
        StreamCommand::Fetch {
            stream,
            partition,
            offset,
            limit,
        } => {
            request(
                client,
                Method::GET,
                &format!(
                    "{base}/v1/streams/{stream}/records?partition={partition}&offset={offset}&limit={limit}"
                ),
                None,
            )
            .await
        }
    }
}

async fn run_queue(
    client: &Client,
    base: &str,
    command: QueueCommand,
) -> Result<Value, Box<dyn std::error::Error>> {
    match command {
        QueueCommand::Create {
            name,
            visibility_timeout_ms,
        } => {
            request(
                client,
                Method::POST,
                &format!("{base}/v1/queues/{name}"),
                Some(json!({
                    "durability": "local_durable",
                    "visibility_timeout_ms": visibility_timeout_ms,
                    "max_messages": 100_000,
                    "retry": {
                        "strategy": "exponential",
                        "initial_delay_ms": 1000,
                        "max_delay_ms": 60000,
                        "jitter_percent": 10,
                        "max_attempts": 8,
                        "max_age_ms": null
                    },
                    "dedupe_window_ms": null
                })),
            )
            .await
        }
        QueueCommand::Send(args) => {
            let (resource, envelope) = make_event(args)?;
            request(
                client,
                Method::POST,
                &format!("{base}/v1/queues/{resource}/messages"),
                Some(serde_json::to_value(envelope)?),
            )
            .await
        }
        QueueCommand::Receive {
            queue,
            consumer,
            max_messages,
            visibility_timeout_ms,
        } => {
            request(
                client,
                Method::POST,
                &format!("{base}/v1/queues/{queue}/acquire"),
                Some(json!({
                    "consumer": consumer,
                    "max_messages": max_messages,
                    "visibility_timeout_ms": visibility_timeout_ms
                })),
            )
            .await
        }
        QueueCommand::Ack { queue, token } => {
            request(
                client,
                Method::POST,
                &format!("{base}/v1/queues/{queue}/settle"),
                Some(json!({"action": "ack", "token": token})),
            )
            .await
        }
        QueueCommand::Release {
            queue,
            token,
            delay_ms,
            reason,
        } => {
            request(
                client,
                Method::POST,
                &format!("{base}/v1/queues/{queue}/settle"),
                Some(json!({
                    "action": "release",
                    "token": token,
                    "delay_ms": delay_ms,
                    "reason": reason
                })),
            )
            .await
        }
    }
}

async fn run_bus(
    client: &Client,
    base: &str,
    command: BusCommand,
) -> Result<Value, Box<dyn std::error::Error>> {
    match command {
        BusCommand::Create { name } => {
            request(
                client,
                Method::POST,
                &format!("{base}/v1/buses/{name}"),
                Some(json!({"durability": "local_durable", "archive": true})),
            )
            .await
        }
        BusCommand::Publish(args) => {
            let (resource, envelope) = make_event(args)?;
            request(
                client,
                Method::POST,
                &format!("{base}/v1/buses/{resource}/events"),
                Some(serde_json::to_value(envelope)?),
            )
            .await
        }
        BusCommand::Replay {
            bus,
            from_ms,
            to_ms,
            event_type,
        } => {
            let mut url = format!("{base}/v1/buses/{bus}/replay?from_ms={from_ms}&to_ms={to_ms}");
            if let Some(event_type) = event_type {
                url.push_str("&event_type=");
                url.push_str(&event_type);
            }
            request(client, Method::GET, &url, None).await
        }
    }
}

fn make_event(
    args: EventArgsWithResource,
) -> Result<(String, EventEnvelope), Box<dyn std::error::Error>> {
    let payload: Value = serde_json::from_str(&args.payload)?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()?;
    let mut event = EventEnvelope::new(args.source, args.event_type, payload, now_ms);
    if let Some(id) = args.id {
        event.id = id;
    }
    event.key = args.key;
    event.dedupe_id = args.dedupe_id;
    Ok((args.resource, event))
}

async fn request(
    client: &Client,
    method: Method,
    url: &str,
    body: Option<Value>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut request = client.request(method, url);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await?;
    let status = response.status();
    if status == StatusCode::NO_CONTENT {
        return Ok(Value::Null);
    }
    let bytes = response.bytes().await?;
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({"raw": String::from_utf8_lossy(&bytes).into_owned()}))
    };
    if status.is_success() {
        Ok(value)
    } else {
        Err(format!("HTTP {status}: {value}").into())
    }
}
