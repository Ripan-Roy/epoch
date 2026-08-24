use std::io::BufRead;

use serde::Serialize;
use thiserror::Error;

use crate::{MAX_FRAME_BYTES, MAX_REQUEST_ITEMS};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Redis,
    Kafka,
    Amqp091,
}

impl Protocol {
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "redis" | "resp" | "resp2" | "resp3" => Some(Self::Redis),
            "kafka" => Some(Self::Kafka),
            "amqp" | "amqp091" | "amqp0-9-1" | "rabbitmq" => Some(Self::Amqp091),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportLevel {
    Supported,
    Partial,
    Unknown,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Assessment {
    pub protocol: Protocol,
    pub feature: String,
    pub level: SupportLevel,
    pub line: usize,
    pub detail: &'static str,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ScanError {
    #[error("usage manifest exceeds the {MAX_FRAME_BYTES}-byte limit")]
    TooLarge,
    #[error("usage manifest has more than {MAX_REQUEST_ITEMS} entries")]
    TooManyEntries,
    #[error("line {line} must start with redis, kafka, or amqp")]
    MissingProtocol { line: usize },
    #[error("line {line} has no feature name")]
    MissingFeature { line: usize },
    #[error("usage manifest could not be read: {0}")]
    Read(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScanReport {
    pub schema: &'static str,
    pub assessments: Vec<Assessment>,
    pub supported: usize,
    pub partial: usize,
    pub unknown: usize,
    pub unsupported: usize,
}

impl ScanReport {
    #[must_use]
    pub const fn fails_at(&self, threshold: SupportLevel) -> bool {
        match threshold {
            SupportLevel::Supported => !self.assessments.is_empty(),
            SupportLevel::Partial => self.partial + self.unknown + self.unsupported > 0,
            SupportLevel::Unknown => self.unknown + self.unsupported > 0,
            SupportLevel::Unsupported => self.unsupported > 0,
        }
    }
}

pub fn scan<R: BufRead>(
    reader: R,
    fixed_protocol: Option<Protocol>,
) -> Result<ScanReport, ScanError> {
    let mut assessments = Vec::new();
    let mut bytes = 0_usize;
    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line.map_err(|error| ScanError::Read(error.to_string()))?;
        bytes = bytes.saturating_add(line.len()).saturating_add(1);
        if bytes > MAX_FRAME_BYTES {
            return Err(ScanError::TooLarge);
        }
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if assessments.len() >= MAX_REQUEST_ITEMS {
            return Err(ScanError::TooManyEntries);
        }
        let (protocol, feature) = if let Some(protocol) = fixed_protocol {
            (protocol, line)
        } else {
            let Some((protocol, feature)) = line.split_once(char::is_whitespace) else {
                return Err(ScanError::MissingFeature { line: line_number });
            };
            (
                Protocol::parse(protocol)
                    .ok_or(ScanError::MissingProtocol { line: line_number })?,
                feature.trim(),
            )
        };
        if feature.is_empty() {
            return Err(ScanError::MissingFeature { line: line_number });
        }
        let (level, detail) = assess(protocol, feature);
        assessments.push(Assessment {
            protocol,
            feature: feature.to_owned(),
            level,
            line: line_number,
            detail,
        });
    }
    let count = |level| {
        assessments
            .iter()
            .filter(|assessment| assessment.level == level)
            .count()
    };
    Ok(ScanReport {
        schema: "epoch.compatibility-scan/v1",
        supported: count(SupportLevel::Supported),
        partial: count(SupportLevel::Partial),
        unknown: count(SupportLevel::Unknown),
        unsupported: count(SupportLevel::Unsupported),
        assessments,
    })
}

#[must_use]
pub fn assess(protocol: Protocol, feature: &str) -> (SupportLevel, &'static str) {
    let tokens = feature
        .split_whitespace()
        .map(|token| {
            token
                .trim_matches(|character: char| matches!(character, '`' | '"' | '\'' | ','))
                .to_ascii_lowercase()
        })
        .collect::<Vec<_>>();
    match protocol {
        Protocol::Redis => assess_redis(&tokens),
        Protocol::Kafka => assess_kafka(&tokens),
        Protocol::Amqp091 => assess_amqp(&tokens),
    }
}

fn assess_redis(tokens: &[String]) -> (SupportLevel, &'static str) {
    let feature = tokens.first().map_or("", String::as_str);
    if feature == "set"
        && tokens
            .iter()
            .skip(1)
            .any(|token| !matches!(token.as_str(), "nx" | "xx" | "get" | "ex" | "px"))
    {
        return (
            SupportLevel::Unsupported,
            "SET option is outside the published EX/PX/NX/XX/GET subset",
        );
    }
    if matches!(feature, "expire" | "pexpire") && tokens.len() > 1 {
        return (
            SupportLevel::Unsupported,
            "conditional expiry options are outside the published beta subset",
        );
    }
    if feature == "select" && tokens.get(1).is_some_and(|database| database != "0") {
        return (
            SupportLevel::Unsupported,
            "only Redis database 0 maps to the configured Epoch Cache",
        );
    }
    match feature {
        "hello" | "auth" | "ping" | "echo" | "quit" | "select" | "client" | "command" | "get"
        | "set" | "del" | "exists" | "mget" | "mset" | "incr" | "decr" | "incrby" | "decrby"
        | "ttl" | "pttl" | "expire" | "pexpire" | "persist" | "type" => {
            (SupportLevel::Supported, "implemented in the RESP gateway")
        }
        "hset" | "hget" | "hdel" | "hgetall" | "lpush" | "rpush" | "lpop" | "rpop" | "sadd"
        | "srem" | "smembers" | "zadd" | "zrem" | "zrange" | "multi" | "exec" | "watch"
        | "eval" | "evalsha" | "publish" | "subscribe" | "xadd" | "xread" | "xgroup" | "blpop"
        | "brpop" => (
            SupportLevel::Unsupported,
            "known Redis feature outside the published beta subset",
        ),
        _ => (
            SupportLevel::Unknown,
            "not present in the published Redis matrix",
        ),
    }
}

fn assess_kafka(tokens: &[String]) -> (SupportLevel, &'static str) {
    let feature = tokens.first().map_or("", String::as_str);
    let supported_range = match feature {
        "produce" => Some(3..=9),
        "fetch" => Some(4..=12),
        "listoffsets" | "offsetfetch" => Some(1..=7),
        "metadata" => Some(1..=12),
        "offsetcommit" => Some(2..=9),
        "findcoordinator" | "apiversions" => Some(0..=4),
        _ => None,
    };
    if let Some(range) = supported_range {
        let Some(version) = tokens.get(1) else {
            return (
                SupportLevel::Partial,
                "API is implemented only for the named versions in the public matrix",
            );
        };
        let Ok(version) = version.parse::<i16>() else {
            return (
                SupportLevel::Unknown,
                "Kafka API version must be a signed 16-bit decimal integer",
            );
        };
        return if range.contains(&version) {
            (
                SupportLevel::Supported,
                "API version is advertised and dispatched by the Kafka gateway",
            )
        } else {
            (
                SupportLevel::Unsupported,
                "API version is outside the advertised Kafka gateway range",
            )
        };
    }
    match feature {
        "joingroup" | "syncgroup" | "heartbeat" | "leavegroup" | "createtopics"
        | "deletetopics" | "initproducerid" | "addpartitionstotxn" | "addoffsetstotxn"
        | "endtxn" | "txnoffsetcommit" | "saslauthenticate" | "saslhandshake" => (
            SupportLevel::Unsupported,
            "known Kafka API outside the published beta subset",
        ),
        _ => (
            SupportLevel::Unknown,
            "not present in the published Kafka API matrix",
        ),
    }
}

fn assess_amqp(tokens: &[String]) -> (SupportLevel, &'static str) {
    let feature = tokens.first().map_or("", String::as_str);
    match feature {
        "connection.open" | "connection.close" | "channel.open" | "channel.close" | "basic.qos"
        | "confirm.select" | "basic.get" | "basic.ack" | "basic.reject" | "basic.nack"
        | "basic.cancel" => (
            SupportLevel::Supported,
            "implemented in the AMQP 0-9-1 gateway",
        ),
        "queue.declare" | "exchange.declare" | "queue.bind" | "basic.publish" | "basic.consume" => {
            (
                SupportLevel::Partial,
                "implemented with the routing and argument boundaries in the public matrix",
            )
        }
        "tx.select" | "tx.commit" | "tx.rollback" | "queue.delete" | "queue.purge"
        | "exchange.delete" | "basic.recover" => (
            SupportLevel::Unsupported,
            "known AMQP method outside the published beta subset",
        ),
        _ => (
            SupportLevel::Unknown,
            "not present in the published AMQP matrix",
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn reports_mixed_protocol_migration_blockers_without_guessing_unknown_features() {
        let report = scan(
            Cursor::new(
                "# observed usage\nredis SET NX PX\nredis EVAL\nkafka Produce 9\nkafka JoinGroup 9\namqp basic.publish\namqp tx.commit\namqp vendor.extension\n",
            ),
            None,
        )
        .unwrap();
        assert_eq!(report.supported, 2);
        assert_eq!(report.partial, 1);
        assert_eq!(report.unsupported, 3);
        assert_eq!(report.unknown, 1);
        assert!(report.fails_at(SupportLevel::Unsupported));
    }

    #[test]
    fn fixed_protocol_manifest_preserves_source_line_numbers() {
        let report = scan(
            Cursor::new("\n# redis monitor\nGET\nHSET\n"),
            Some(Protocol::Redis),
        )
        .unwrap();
        assert_eq!(report.assessments[0].line, 3);
        assert_eq!(report.assessments[1].level, SupportLevel::Unsupported);
    }

    #[test]
    fn fails_closed_on_ambiguous_auto_protocol_lines() {
        assert_eq!(
            scan(Cursor::new("SET\n"), None),
            Err(ScanError::MissingFeature { line: 1 })
        );
        assert_eq!(
            scan(Cursor::new("unknown SET\n"), None),
            Err(ScanError::MissingProtocol { line: 1 })
        );
    }

    #[test]
    fn rejects_unsupported_kafka_versions_and_redis_options() {
        let report = scan(
            Cursor::new(
                "kafka Produce 9\nkafka Produce 10\nkafka Fetch 4\nkafka Fetch latest\nredis SET NX PX\nredis SET KEEPTTL\nredis EXPIRE NX\nredis SELECT 1\n",
            ),
            None,
        )
        .unwrap();
        assert_eq!(report.supported, 3);
        assert_eq!(report.unknown, 1);
        assert_eq!(report.unsupported, 4);
    }

    #[test]
    fn bounds_manifest_entry_count() {
        let manifest = "redis GET\n".repeat(MAX_REQUEST_ITEMS + 1);
        assert_eq!(
            scan(Cursor::new(manifest), None),
            Err(ScanError::TooManyEntries)
        );
    }
}
