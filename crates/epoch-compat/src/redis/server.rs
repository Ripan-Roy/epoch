use std::{
    fmt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    CompatibilityBackend, MAX_FRAME_BYTES,
    backend::{BackendError, CacheValue},
};

use super::protocol::{RespDecodeError, RespValue, decode_request, encode_response};

#[derive(Clone)]
pub struct RedisConfig {
    pub cache: String,
    pub password: Option<String>,
    pub max_connections: usize,
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisConfig")
            .field("cache", &self.cache)
            .field("password_configured", &self.password.is_some())
            .field("max_connections", &self.max_connections)
            .finish()
    }
}

#[derive(Debug)]
pub struct RedisServer<B> {
    backend: Arc<B>,
    config: RedisConfig,
}

impl<B: CompatibilityBackend> RedisServer<B> {
    pub fn new(backend: Arc<B>, config: RedisConfig) -> Result<Self, BackendError> {
        if config.cache.trim().is_empty() || config.max_connections == 0 {
            return Err(BackendError::Invalid(
                "Redis cache and positive connection limit are required".into(),
            ));
        }
        Ok(Self { backend, config })
    }

    pub async fn serve(self, listener: TcpListener) -> Result<(), std::io::Error> {
        let permits = Arc::new(tokio::sync::Semaphore::new(self.config.max_connections));
        loop {
            let (stream, _) = listener.accept().await?;
            let permit = Arc::clone(&permits).acquire_owned().await;
            let Ok(permit) = permit else {
                return Ok(());
            };
            let backend = Arc::clone(&self.backend);
            let config = self.config.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) =
                    serve_connection(stream, RedisSession::new(backend, config)).await
                {
                    tracing::warn!(protocol = "redis", %error, "compatibility connection closed");
                }
            });
        }
    }
}

async fn serve_connection<B: CompatibilityBackend>(
    mut stream: TcpStream,
    mut session: RedisSession<B>,
) -> Result<(), std::io::Error> {
    let mut buffer = Vec::with_capacity(16 * 1024);
    loop {
        match decode_request(&buffer) {
            Ok((arguments, consumed)) => {
                buffer.drain(..consumed);
                let response = session.execute(arguments).await;
                stream
                    .write_all(&encode_response(&response, session.resp3))
                    .await?;
                if session.quit {
                    return Ok(());
                }
            }
            Err(RespDecodeError::Incomplete) => {
                if buffer.len() >= MAX_FRAME_BYTES {
                    stream
                        .write_all(b"-ERR protocol frame exceeds limit\r\n")
                        .await?;
                    return Ok(());
                }
                if stream.read_buf(&mut buffer).await? == 0 {
                    return Ok(());
                }
            }
            Err(RespDecodeError::Protocol(error)) => {
                stream
                    .write_all(&encode_response(
                        &RespValue::Error(format!("ERR {error}")),
                        false,
                    ))
                    .await?;
                return Ok(());
            }
        }
    }
}

#[derive(Debug)]
pub struct RedisSession<B> {
    backend: Arc<B>,
    config: RedisConfig,
    resp3: bool,
    authenticated: bool,
    client_name: Option<String>,
    quit: bool,
}

impl<B: CompatibilityBackend> RedisSession<B> {
    pub fn new(backend: Arc<B>, config: RedisConfig) -> Self {
        let authenticated = config.password.is_none();
        Self {
            backend,
            config,
            resp3: false,
            authenticated,
            client_name: None,
            quit: false,
        }
    }

    pub async fn execute(&mut self, arguments: Vec<Vec<u8>>) -> RespValue {
        let Some(command) = arguments
            .first()
            .and_then(|value| std::str::from_utf8(value).ok())
        else {
            return error("command must be UTF-8");
        };
        let command = command.to_ascii_uppercase();
        if !self.authenticated && !matches!(command.as_str(), "AUTH" | "HELLO" | "QUIT") {
            return RespValue::Error("NOAUTH Authentication required.".into());
        }
        match command.as_str() {
            "HELLO" => self.hello(&arguments[1..]),
            "AUTH" => self.auth(&arguments[1..]),
            "PING" => ping(&arguments[1..]),
            "ECHO" => one_bulk("echo", &arguments[1..]),
            "QUIT" => {
                self.quit = true;
                RespValue::Simple("OK".into())
            }
            "SELECT" => select(&arguments[1..]),
            "CLIENT" => self.client(&arguments[1..]),
            "COMMAND" => command_metadata(&arguments[1..]),
            "GET" => self.get(&arguments[1..]).await,
            "SET" => self.set(&arguments[1..]).await,
            "DEL" => self.delete(&arguments[1..]).await,
            "EXISTS" => self.exists(&arguments[1..]).await,
            "MGET" => self.mget(&arguments[1..]).await,
            "MSET" => self.mset(&arguments[1..]).await,
            "INCR" => self.increment(&arguments[1..], 1).await,
            "DECR" => self.increment(&arguments[1..], -1).await,
            "INCRBY" => self.increment_by(&arguments[1..], 1).await,
            "DECRBY" => self.increment_by(&arguments[1..], -1).await,
            "TTL" => self.ttl(&arguments[1..], false).await,
            "PTTL" => self.ttl(&arguments[1..], true).await,
            "EXPIRE" => self.expire(&arguments[1..], 1_000).await,
            "PEXPIRE" => self.expire(&arguments[1..], 1).await,
            "PERSIST" => self.persist(&arguments[1..]).await,
            "TYPE" => self.value_type(&arguments[1..]).await,
            _ => RespValue::Error(format!(
                "ERR unknown command '{}'; see Epoch compatibility matrix",
                command.to_ascii_lowercase()
            )),
        }
    }

    fn hello(&mut self, args: &[Vec<u8>]) -> RespValue {
        let Some(version) = args.first().and_then(|value| text(value)) else {
            return error("HELLO requires protocol version 2 or 3");
        };
        if version != "2" && version != "3" {
            return RespValue::Error("NOPROTO unsupported protocol version".into());
        }
        let mut index = 1;
        while index < args.len() {
            match upper(&args[index]).as_deref() {
                Some("AUTH") if index + 2 < args.len() => {
                    if !self.check_password(&args[index + 2]) {
                        return RespValue::Error("WRONGPASS invalid username-password pair".into());
                    }
                    self.authenticated = true;
                    index += 3;
                }
                Some("SETNAME") if index + 1 < args.len() => {
                    self.client_name = text(&args[index + 1]).map(str::to_owned);
                    index += 2;
                }
                _ => return error("invalid HELLO option"),
            }
        }
        if !self.authenticated {
            return RespValue::Error("NOAUTH HELLO must be called with the client password".into());
        }
        self.resp3 = version == "3";
        RespValue::Map(vec![
            bulk_pair("server", "epoch"),
            bulk_pair("version", env!("CARGO_PKG_VERSION")),
            (
                bulk("proto"),
                RespValue::Integer(if self.resp3 { 3 } else { 2 }),
            ),
            (bulk("id"), RespValue::Integer(1)),
            bulk_pair("mode", "standalone"),
            bulk_pair("role", "master"),
        ])
    }

    fn auth(&mut self, args: &[Vec<u8>]) -> RespValue {
        let ([password] | [_, password]) = args else {
            return arity("auth");
        };
        if !self.check_password(password) {
            return RespValue::Error("WRONGPASS invalid username-password pair".into());
        }
        self.authenticated = true;
        RespValue::Simple("OK".into())
    }

    fn check_password(&self, actual: &[u8]) -> bool {
        self.config
            .password
            .as_ref()
            .is_none_or(|expected| constant_time_equal(expected.as_bytes(), actual))
    }

    fn client(&mut self, args: &[Vec<u8>]) -> RespValue {
        match args.first().and_then(|value| upper(value)).as_deref() {
            Some("SETNAME") if args.len() == 2 => {
                self.client_name = text(&args[1]).map(str::to_owned);
                RespValue::Simple("OK".into())
            }
            Some("GETNAME") if args.len() == 1 => self
                .client_name
                .as_ref()
                .map_or(RespValue::Null, |name| bulk(name)),
            Some("SETINFO" | "MAINT_NOTIFICATIONS") => RespValue::Simple("OK".into()),
            Some("ID") if args.len() == 1 => RespValue::Integer(1),
            _ => error("unsupported CLIENT subcommand"),
        }
    }

    async fn get(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("get");
        };
        match self.backend.cache_get(&self.config.cache, key).await {
            Ok(Some(entry)) => cache_value(entry.value),
            Ok(None) => RespValue::Null,
            Err(error) => backend_error(error),
        }
    }

    async fn set(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() < 2 {
            return arity("set");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let mut ttl_ms = None;
        let mut only_if_absent = false;
        let mut only_if_present = false;
        let mut return_previous = false;
        let mut index = 2;
        while index < args.len() {
            match upper(&args[index]).as_deref() {
                Some("NX") => {
                    only_if_absent = true;
                    index += 1;
                }
                Some("XX") => {
                    only_if_present = true;
                    index += 1;
                }
                Some("GET") => {
                    return_previous = true;
                    index += 1;
                }
                Some("EX") if index + 1 < args.len() => {
                    let Some(value) =
                        positive_u64(&args[index + 1]).and_then(|value| value.checked_mul(1_000))
                    else {
                        return error("invalid expire time in 'set' command");
                    };
                    ttl_ms = Some(value);
                    index += 2;
                }
                Some("PX") if index + 1 < args.len() => {
                    let Some(value) = positive_u64(&args[index + 1]) else {
                        return error("invalid expire time in 'set' command");
                    };
                    ttl_ms = Some(value);
                    index += 2;
                }
                _ => return error("syntax error"),
            }
        }
        if only_if_absent && only_if_present {
            return error("syntax error");
        }
        let previous = if return_previous {
            self.backend
                .cache_get(&self.config.cache, key)
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        match self
            .backend
            .cache_set(
                &self.config.cache,
                key,
                CacheValue::Blob(args[1].clone()),
                ttl_ms,
                only_if_absent,
                only_if_present,
            )
            .await
        {
            Ok(Some(_)) if return_previous => {
                previous.map_or(RespValue::Null, |entry| cache_value(entry.value))
            }
            Ok(Some(_)) => RespValue::Simple("OK".into()),
            Ok(None) => RespValue::Null,
            Err(error) => backend_error(error),
        }
    }

    async fn delete(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(keys) = keys(args) else {
            return arity("del");
        };
        match self.backend.cache_delete(&self.config.cache, &keys).await {
            Ok(count) => integer(count),
            Err(error) => backend_error(error),
        }
    }

    async fn exists(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(keys) = keys(args) else {
            return arity("exists");
        };
        let mut count = 0_u64;
        for key in keys {
            match self.backend.cache_get(&self.config.cache, &key).await {
                Ok(Some(_)) => count += 1,
                Ok(None) => {}
                Err(error) => return backend_error(error),
            }
        }
        integer(count)
    }

    async fn mget(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(keys) = keys(args) else {
            return arity("mget");
        };
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            match self.backend.cache_get(&self.config.cache, &key).await {
                Ok(Some(entry)) => values.push(cache_value(entry.value)),
                Ok(None) => values.push(RespValue::Null),
                Err(error) => return backend_error(error),
            }
        }
        RespValue::Array(values)
    }

    async fn mset(&self, args: &[Vec<u8>]) -> RespValue {
        if args.is_empty() || !args.len().is_multiple_of(2) {
            return arity("mset");
        }
        for pair in args.chunks_exact(2) {
            let Some(key) = text(&pair[0]) else {
                return error("key must be UTF-8");
            };
            if let Err(error) = self
                .backend
                .cache_set(
                    &self.config.cache,
                    key,
                    CacheValue::Blob(pair[1].clone()),
                    None,
                    false,
                    false,
                )
                .await
            {
                return backend_error(error);
            }
        }
        RespValue::Simple("OK".into())
    }

    async fn increment(&self, args: &[Vec<u8>], delta: i64) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("incr");
        };
        match self
            .backend
            .cache_increment(&self.config.cache, key, delta)
            .await
        {
            Ok(value) => RespValue::Integer(value),
            Err(error) => backend_error(error),
        }
    }

    async fn increment_by(&self, args: &[Vec<u8>], direction: i64) -> RespValue {
        if args.len() != 2 {
            return arity("incrby");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let Some(delta) = signed_i64(&args[1]).and_then(|value| value.checked_mul(direction))
        else {
            return error("value is not an integer or out of range");
        };
        match self
            .backend
            .cache_increment(&self.config.cache, key, delta)
            .await
        {
            Ok(value) => RespValue::Integer(value),
            Err(error) => backend_error(error),
        }
    }

    async fn ttl(&self, args: &[Vec<u8>], milliseconds: bool) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("ttl");
        };
        match self.backend.cache_get(&self.config.cache, key).await {
            Ok(None) => RespValue::Integer(-2),
            Ok(Some(entry)) => match entry.expires_at_ms {
                None => RespValue::Integer(-1),
                Some(expiry) => {
                    let remaining = expiry.saturating_sub(now_ms());
                    integer(if milliseconds {
                        remaining
                    } else {
                        remaining / 1_000
                    })
                }
            },
            Err(error) => backend_error(error),
        }
    }

    async fn expire(&self, args: &[Vec<u8>], multiplier: u64) -> RespValue {
        if args.len() != 2 {
            return arity("expire");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let Some(ttl) = positive_u64(&args[1]).and_then(|value| value.checked_mul(multiplier))
        else {
            return error("value is not an integer or out of range");
        };
        match self
            .backend
            .cache_expire(&self.config.cache, key, Some(ttl))
            .await
        {
            Ok(changed) => RespValue::Integer(i64::from(changed)),
            Err(error) => backend_error(error),
        }
    }

    async fn persist(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("persist");
        };
        match self
            .backend
            .cache_expire(&self.config.cache, key, None)
            .await
        {
            Ok(changed) => RespValue::Integer(i64::from(changed)),
            Err(error) => backend_error(error),
        }
    }

    async fn value_type(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("type");
        };
        match self.backend.cache_get(&self.config.cache, key).await {
            Ok(None) => RespValue::Simple("none".into()),
            Ok(Some(entry)) => RespValue::Simple(
                match entry.value {
                    CacheValue::String(_) | CacheValue::Blob(_) | CacheValue::Counter(_) => {
                        "string"
                    }
                    CacheValue::Hash(_) => "hash",
                    CacheValue::List(_) => "list",
                    CacheValue::Set(_) => "set",
                    CacheValue::SortedSet(_) => "zset",
                }
                .into(),
            ),
            Err(error) => backend_error(error),
        }
    }
}

fn ping(args: &[Vec<u8>]) -> RespValue {
    match args {
        [] => RespValue::Simple("PONG".into()),
        [value] => RespValue::Bulk(value.clone()),
        _ => arity("ping"),
    }
}

fn one_bulk(command: &str, args: &[Vec<u8>]) -> RespValue {
    match args {
        [value] => RespValue::Bulk(value.clone()),
        _ => arity(command),
    }
}

fn select(args: &[Vec<u8>]) -> RespValue {
    match args {
        [value] if value == b"0" => RespValue::Simple("OK".into()),
        [_] => error("DB index is out of range"),
        _ => arity("select"),
    }
}

fn command_metadata(args: &[Vec<u8>]) -> RespValue {
    match args.first().and_then(|value| upper(value)).as_deref() {
        None | Some("INFO" | "DOCS") => RespValue::Array(Vec::new()),
        Some("COUNT") => RespValue::Integer(22),
        _ => error("unsupported COMMAND subcommand"),
    }
}

fn cache_value(value: CacheValue) -> RespValue {
    match value {
        CacheValue::String(value) => RespValue::Bulk(value.into_bytes()),
        CacheValue::Blob(value) => RespValue::Bulk(value),
        CacheValue::Counter(value) => RespValue::Bulk(value.to_string().into_bytes()),
        _ => RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        ),
    }
}

fn backend_error(error_value: BackendError) -> RespValue {
    match error_value {
        BackendError::NotFound => RespValue::Null,
        BackendError::Conflict => error("operation conflicted"),
        BackendError::Invalid(detail) => RespValue::Error(format!("ERR {detail}")),
        BackendError::Unavailable(_) => {
            RespValue::Error("TRYAGAIN Epoch backend is unavailable".into())
        }
    }
}

fn error(message: &str) -> RespValue {
    RespValue::Error(format!("ERR {message}"))
}

fn arity(command: &str) -> RespValue {
    error(&format!(
        "wrong number of arguments for '{command}' command"
    ))
}

fn text(value: &[u8]) -> Option<&str> {
    std::str::from_utf8(value).ok()
}

fn upper(value: &[u8]) -> Option<String> {
    text(value).map(str::to_ascii_uppercase)
}

fn exact_key(args: &[Vec<u8>]) -> Option<&str> {
    match args {
        [key] => text(key),
        _ => None,
    }
}

fn keys(args: &[Vec<u8>]) -> Option<Vec<String>> {
    if args.is_empty() {
        return None;
    }
    args.iter()
        .map(|value| text(value).map(str::to_owned))
        .collect()
}

fn positive_u64(value: &[u8]) -> Option<u64> {
    text(value)?.parse().ok().filter(|value| *value > 0)
}

fn signed_i64(value: &[u8]) -> Option<i64> {
    text(value)?.parse().ok()
}

fn integer(value: u64) -> RespValue {
    RespValue::Integer(i64::try_from(value).unwrap_or(i64::MAX))
}

fn bulk(value: &str) -> RespValue {
    RespValue::Bulk(value.as_bytes().to_vec())
}

fn bulk_pair(key: &str, value: &str) -> (RespValue, RespValue) {
    (bulk(key), bulk(value))
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn constant_time_equal(expected: &[u8], actual: &[u8]) -> bool {
    let mut difference = expected.len() ^ actual.len();
    for index in 0..expected.len().max(actual.len()) {
        difference |= usize::from(
            expected.get(index).copied().unwrap_or(0) ^ actual.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MemoryBackend;

    #[test]
    fn configuration_debug_never_exposes_the_redis_password() {
        let config = RedisConfig {
            cache: "sessions".into(),
            password: Some("redis-super-secret".into()),
            max_connections: 8,
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("redis-super-secret"));
        assert!(debug.contains("password_configured: true"));
    }

    fn command(values: &[&[u8]]) -> Vec<Vec<u8>> {
        values.iter().map(|value| value.to_vec()).collect()
    }

    fn session(password: Option<&str>) -> RedisSession<MemoryBackend> {
        RedisSession::new(
            Arc::new(MemoryBackend::with_resources(
                "sessions", "events", 2, "jobs",
            )),
            RedisConfig {
                cache: "sessions".into(),
                password: password.map(str::to_owned),
                max_connections: 4,
            },
        )
    }

    #[tokio::test]
    async fn executes_binary_safe_string_ttl_and_conditional_commands() {
        let mut session = session(None);
        assert_eq!(
            session
                .execute(command(&[b"SET", b"key", b"a\0b", b"PX", b"5000"]))
                .await,
            RespValue::Simple("OK".into())
        );
        assert_eq!(
            session.execute(command(&[b"GET", b"key"])).await,
            RespValue::Bulk(b"a\0b".to_vec())
        );
        assert_eq!(
            session
                .execute(command(&[b"SET", b"key", b"other", b"NX"]))
                .await,
            RespValue::Null
        );
        assert!(matches!(
            session.execute(command(&[b"PTTL", b"key"])).await,
            RespValue::Integer(1..=5_000)
        ));
        assert_eq!(
            session.execute(command(&[b"PERSIST", b"key"])).await,
            RespValue::Integer(1)
        );
        assert_eq!(
            session.execute(command(&[b"TTL", b"key"])).await,
            RespValue::Integer(-1)
        );
    }

    #[tokio::test]
    async fn authenticates_hello_and_handles_pipeline_safe_session_state() {
        let mut session = session(Some("correct horse"));
        assert_eq!(
            session.execute(command(&[b"GET", b"key"])).await,
            RespValue::Error("NOAUTH Authentication required.".into())
        );
        assert!(matches!(
            session
                .execute(command(&[
                    b"HELLO",
                    b"3",
                    b"AUTH",
                    b"default",
                    b"correct horse",
                    b"SETNAME",
                    b"integration-test",
                ]))
                .await,
            RespValue::Map(_)
        ));
        assert!(session.resp3);
        assert_eq!(
            session.execute(command(&[b"CLIENT", b"GETNAME"])).await,
            RespValue::Bulk(b"integration-test".to_vec())
        );
    }

    #[tokio::test]
    async fn increments_and_reports_type_errors_without_mutating_the_value() {
        let mut session = session(None);
        assert_eq!(
            session
                .execute(command(&[b"INCRBY", b"count", b"41"]))
                .await,
            RespValue::Integer(41)
        );
        assert_eq!(
            session.execute(command(&[b"INCR", b"count"])).await,
            RespValue::Integer(42)
        );
        assert_eq!(
            session.execute(command(&[b"SET", b"name", b"epoch"])).await,
            RespValue::Simple("OK".into())
        );
        assert!(matches!(
            session.execute(command(&[b"INCR", b"name"])).await,
            RespValue::Error(message) if message.contains("not an integer")
        ));
        assert_eq!(
            session.execute(command(&[b"GET", b"name"])).await,
            RespValue::Bulk(b"epoch".to_vec())
        );
    }
}
