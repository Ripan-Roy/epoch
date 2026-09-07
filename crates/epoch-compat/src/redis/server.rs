use std::{
    collections::BTreeMap,
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
    backend::{
        BackendError, CacheCollectionMutation, CacheCollectionResult, CacheSetCondition,
        CacheSetOptions, CacheValue,
    },
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

    #[allow(
        clippy::too_many_lines,
        reason = "the explicit command table is the fail-closed Redis compatibility boundary"
    )]
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
            "HSET" => self.hash_set(&arguments[1..]).await,
            "HGET" => self.hash_get(&arguments[1..]).await,
            "HMGET" => self.hash_multi_get(&arguments[1..]).await,
            "HDEL" => self.hash_delete(&arguments[1..]).await,
            "HEXISTS" => self.hash_exists(&arguments[1..]).await,
            "HLEN" => self.hash_length(&arguments[1..]).await,
            "HGETALL" => self.hash_get_all(&arguments[1..]).await,
            "LPUSH" => self.list_push(&arguments[1..], true).await,
            "RPUSH" => self.list_push(&arguments[1..], false).await,
            "LPOP" => self.list_pop(&arguments[1..], true).await,
            "RPOP" => self.list_pop(&arguments[1..], false).await,
            "LLEN" => self.list_length(&arguments[1..]).await,
            "LRANGE" => self.list_range(&arguments[1..]).await,
            "LINDEX" => self.list_index(&arguments[1..]).await,
            "SADD" => self.set_add(&arguments[1..]).await,
            "SREM" => self.set_remove(&arguments[1..]).await,
            "SMEMBERS" => self.set_members(&arguments[1..]).await,
            "SCARD" => self.set_cardinality(&arguments[1..]).await,
            "SISMEMBER" => self.set_is_member(&arguments[1..]).await,
            "ZADD" => self.sorted_set_add(&arguments[1..]).await,
            "ZREM" => self.sorted_set_remove(&arguments[1..]).await,
            "ZCARD" => self.sorted_set_cardinality(&arguments[1..]).await,
            "ZSCORE" => self.sorted_set_score(&arguments[1..]).await,
            "ZRANGE" => self.sorted_set_range(&arguments[1..]).await,
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
        let condition = if only_if_absent {
            CacheSetCondition::Missing
        } else if only_if_present {
            CacheSetCondition::Present
        } else {
            CacheSetCondition::Always
        };
        match self
            .backend
            .cache_set(
                &self.config.cache,
                key,
                CacheValue::Blob(args[1].clone()),
                CacheSetOptions {
                    ttl_ms,
                    condition,
                    return_previous,
                },
            )
            .await
        {
            Ok(outcome) if return_previous => outcome
                .previous
                .map_or(RespValue::Null, |entry| cache_value(entry.value)),
            Ok(outcome) if outcome.applied => RespValue::Simple("OK".into()),
            Ok(_) => RespValue::Null,
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
                    CacheSetOptions::default(),
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

    async fn hash_set(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return arity("hset");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let mut entries = BTreeMap::new();
        for pair in args[1..].chunks_exact(2) {
            let (Some(field), Some(value)) = (text(&pair[0]), text(&pair[1])) else {
                return error("hash fields and values must be UTF-8");
            };
            entries.insert(field.to_owned(), value.to_owned());
        }
        match self
            .backend
            .cache_collection_mutate(
                &self.config.cache,
                key,
                CacheCollectionMutation::HashSet { entries },
            )
            .await
        {
            Ok(CacheCollectionResult::HashSet { added }) => integer(added),
            Ok(_) => unexpected_collection_result(),
            Err(error) => backend_error(error),
        }
    }

    async fn hash_get(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() != 2 {
            return arity("hget");
        }
        let (Some(key), Some(field)) = (text(&args[0]), text(&args[1])) else {
            return error("key and field must be UTF-8");
        };
        match self.hash_value(key).await {
            Ok(Some(hash)) => hash.get(field).map_or(RespValue::Null, |value| bulk(value)),
            Ok(None) => RespValue::Null,
            Err(error) => backend_error(error),
        }
    }

    async fn hash_multi_get(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() < 2 {
            return arity("hmget");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let Some(fields) = utf8_values(&args[1..]) else {
            return error("hash fields must be UTF-8");
        };
        match self.hash_value(key).await {
            Ok(hash) => RespValue::Array(
                fields
                    .iter()
                    .map(|field| {
                        hash.as_ref()
                            .and_then(|hash| hash.get(field))
                            .map_or(RespValue::Null, |value| bulk(value))
                    })
                    .collect(),
            ),
            Err(error) => backend_error(error),
        }
    }

    async fn hash_delete(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() < 2 {
            return arity("hdel");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let Some(fields) = utf8_values(&args[1..]) else {
            return error("hash fields must be UTF-8");
        };
        match self
            .backend
            .cache_collection_mutate(
                &self.config.cache,
                key,
                CacheCollectionMutation::HashDelete { fields },
            )
            .await
        {
            Ok(CacheCollectionResult::HashDelete { removed }) => integer(removed),
            Ok(_) => unexpected_collection_result(),
            Err(error) => backend_error(error),
        }
    }

    async fn hash_exists(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() != 2 {
            return arity("hexists");
        }
        let (Some(key), Some(field)) = (text(&args[0]), text(&args[1])) else {
            return error("key and field must be UTF-8");
        };
        match self.hash_value(key).await {
            Ok(value) => RespValue::Integer(i64::from(
                value.is_some_and(|hash| hash.contains_key(field)),
            )),
            Err(error) => backend_error(error),
        }
    }

    async fn hash_length(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("hlen");
        };
        match self.hash_value(key).await {
            Ok(value) => integer(value.map_or(0, |hash| count_u64(hash.len()))),
            Err(error) => backend_error(error),
        }
    }

    async fn hash_get_all(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("hgetall");
        };
        match self.hash_value(key).await {
            Ok(Some(hash)) if self.resp3 => RespValue::Map(
                hash.into_iter()
                    .map(|(field, value)| (bulk(&field), bulk(&value)))
                    .collect(),
            ),
            Ok(Some(hash)) => RespValue::Array(
                hash.into_iter()
                    .flat_map(|(field, value)| [bulk(&field), bulk(&value)])
                    .collect(),
            ),
            Ok(None) => RespValue::Array(Vec::new()),
            Err(error) => backend_error(error),
        }
    }

    async fn list_push(&self, args: &[Vec<u8>], front: bool) -> RespValue {
        if args.len() < 2 {
            return arity(if front { "lpush" } else { "rpush" });
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let Some(values) = utf8_values(&args[1..]) else {
            return error("list values must be UTF-8");
        };
        match self
            .backend
            .cache_collection_mutate(
                &self.config.cache,
                key,
                CacheCollectionMutation::ListPush { values, front },
            )
            .await
        {
            Ok(CacheCollectionResult::ListPush { length }) => integer(length),
            Ok(_) => unexpected_collection_result(),
            Err(error) => backend_error(error),
        }
    }

    async fn list_pop(&self, args: &[Vec<u8>], front: bool) -> RespValue {
        if !(1..=2).contains(&args.len()) {
            return arity(if front { "lpop" } else { "rpop" });
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let with_count = args.len() == 2;
        let count = if let Some(value) = args.get(1) {
            let Some(count) = bounded_count(value) else {
                return error("value is out of range, must be positive");
            };
            count
        } else {
            1
        };
        match self
            .backend
            .cache_collection_mutate(
                &self.config.cache,
                key,
                CacheCollectionMutation::ListPop { count, front },
            )
            .await
        {
            Ok(CacheCollectionResult::ListPop { values }) if values.is_empty() => RespValue::Null,
            Ok(CacheCollectionResult::ListPop { values }) if with_count => {
                RespValue::Array(values.into_iter().map(|value| bulk(&value)).collect())
            }
            Ok(CacheCollectionResult::ListPop { mut values }) => bulk(&values.remove(0)),
            Ok(_) => unexpected_collection_result(),
            Err(error) => backend_error(error),
        }
    }

    async fn list_length(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("llen");
        };
        match self.list_value(key).await {
            Ok(value) => integer(value.map_or(0, |list| count_u64(list.len()))),
            Err(error) => backend_error(error),
        }
    }

    async fn list_range(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() != 3 {
            return arity("lrange");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let (Some(start), Some(stop)) = (signed_i64(&args[1]), signed_i64(&args[2])) else {
            return error("value is not an integer or out of range");
        };
        match self.list_value(key).await {
            Ok(Some(list)) => {
                let Some((start, end)) = redis_range(list.len(), start, stop) else {
                    return RespValue::Array(Vec::new());
                };
                RespValue::Array(list[start..end].iter().map(|value| bulk(value)).collect())
            }
            Ok(None) => RespValue::Array(Vec::new()),
            Err(error) => backend_error(error),
        }
    }

    async fn list_index(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() != 2 {
            return arity("lindex");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let Some(index) = signed_i64(&args[1]) else {
            return error("value is not an integer or out of range");
        };
        match self.list_value(key).await {
            Ok(Some(list)) => redis_index(list.len(), index)
                .and_then(|index| list.get(index))
                .map_or(RespValue::Null, |value| bulk(value)),
            Ok(None) => RespValue::Null,
            Err(error) => backend_error(error),
        }
    }

    async fn set_add(&self, args: &[Vec<u8>]) -> RespValue {
        self.set_mutation(args, true).await
    }

    async fn set_remove(&self, args: &[Vec<u8>]) -> RespValue {
        self.set_mutation(args, false).await
    }

    async fn set_mutation(&self, args: &[Vec<u8>], add: bool) -> RespValue {
        if args.len() < 2 {
            return arity(if add { "sadd" } else { "srem" });
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let Some(members) = utf8_values(&args[1..]) else {
            return error("set members must be UTF-8");
        };
        let mutation = if add {
            CacheCollectionMutation::SetAdd { members }
        } else {
            CacheCollectionMutation::SetRemove { members }
        };
        match self
            .backend
            .cache_collection_mutate(&self.config.cache, key, mutation)
            .await
        {
            Ok(CacheCollectionResult::SetAdd { added }) => integer(added),
            Ok(CacheCollectionResult::SetRemove { removed }) => integer(removed),
            Ok(_) => unexpected_collection_result(),
            Err(error) => backend_error(error),
        }
    }

    async fn set_members(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("smembers");
        };
        match self.set_value(key).await {
            Ok(value) => {
                let members = value
                    .unwrap_or_default()
                    .into_iter()
                    .map(|member| bulk(&member))
                    .collect();
                if self.resp3 {
                    RespValue::Set(members)
                } else {
                    RespValue::Array(members)
                }
            }
            Err(error) => backend_error(error),
        }
    }

    async fn set_cardinality(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("scard");
        };
        match self.set_value(key).await {
            Ok(value) => integer(value.map_or(0, |set| count_u64(set.len()))),
            Err(error) => backend_error(error),
        }
    }

    async fn set_is_member(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() != 2 {
            return arity("sismember");
        }
        let (Some(key), Some(member)) = (text(&args[0]), text(&args[1])) else {
            return error("key and member must be UTF-8");
        };
        match self.set_value(key).await {
            Ok(value) => RespValue::Integer(i64::from(value.is_some_and(|set| {
                set.binary_search_by(|value| value.as_str().cmp(member))
                    .is_ok()
            }))),
            Err(error) => backend_error(error),
        }
    }

    async fn sorted_set_add(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return arity("zadd");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let mut entries = BTreeMap::new();
        for pair in args[1..].chunks_exact(2) {
            let (Some(score), Some(member)) = (finite_f64(&pair[0]), text(&pair[1])) else {
                return error("score is not a finite floating point number");
            };
            entries.insert(member.to_owned(), score);
        }
        match self
            .backend
            .cache_collection_mutate(
                &self.config.cache,
                key,
                CacheCollectionMutation::SortedSetAdd { entries },
            )
            .await
        {
            Ok(CacheCollectionResult::SortedSetAdd { added }) => integer(added),
            Ok(_) => unexpected_collection_result(),
            Err(error) => backend_error(error),
        }
    }

    async fn sorted_set_remove(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() < 2 {
            return arity("zrem");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let Some(members) = utf8_values(&args[1..]) else {
            return error("sorted-set members must be UTF-8");
        };
        match self
            .backend
            .cache_collection_mutate(
                &self.config.cache,
                key,
                CacheCollectionMutation::SortedSetRemove { members },
            )
            .await
        {
            Ok(CacheCollectionResult::SortedSetRemove { removed }) => integer(removed),
            Ok(_) => unexpected_collection_result(),
            Err(error) => backend_error(error),
        }
    }

    async fn sorted_set_cardinality(&self, args: &[Vec<u8>]) -> RespValue {
        let Some(key) = exact_key(args) else {
            return arity("zcard");
        };
        match self.sorted_set_value(key).await {
            Ok(value) => integer(value.map_or(0, |set| count_u64(set.len()))),
            Err(error) => backend_error(error),
        }
    }

    async fn sorted_set_score(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() != 2 {
            return arity("zscore");
        }
        let (Some(key), Some(member)) = (text(&args[0]), text(&args[1])) else {
            return error("key and member must be UTF-8");
        };
        match self.sorted_set_value(key).await {
            Ok(Some(set)) => set
                .get(member)
                .map_or(RespValue::Null, |score| bulk(&score.to_string())),
            Ok(None) => RespValue::Null,
            Err(error) => backend_error(error),
        }
    }

    async fn sorted_set_range(&self, args: &[Vec<u8>]) -> RespValue {
        if !(3..=4).contains(&args.len()) {
            return arity("zrange");
        }
        let Some(key) = text(&args[0]) else {
            return error("key must be UTF-8");
        };
        let (Some(start), Some(stop)) = (signed_i64(&args[1]), signed_i64(&args[2])) else {
            return error("value is not an integer or out of range");
        };
        let with_scores = match args.get(3) {
            None => false,
            Some(option) if upper(option).as_deref() == Some("WITHSCORES") => true,
            Some(_) => return error("syntax error"),
        };
        match self.sorted_set_value(key).await {
            Ok(value) => {
                let mut values = value.unwrap_or_default().into_iter().collect::<Vec<_>>();
                values.sort_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| left.0.cmp(&right.0))
                });
                let Some((start, end)) = redis_range(values.len(), start, stop) else {
                    return RespValue::Array(Vec::new());
                };
                let values = &values[start..end];
                if with_scores && self.resp3 {
                    RespValue::Array(
                        values
                            .iter()
                            .map(|(member, score)| {
                                RespValue::Array(vec![bulk(member), RespValue::Double(*score)])
                            })
                            .collect(),
                    )
                } else if with_scores {
                    RespValue::Array(
                        values
                            .iter()
                            .flat_map(|(member, score)| [bulk(member), bulk(&score.to_string())])
                            .collect(),
                    )
                } else {
                    RespValue::Array(values.iter().map(|(member, _)| bulk(member)).collect())
                }
            }
            Err(error) => backend_error(error),
        }
    }

    async fn hash_value(
        &self,
        key: &str,
    ) -> Result<Option<BTreeMap<String, String>>, BackendError> {
        match self.backend.cache_get(&self.config.cache, key).await? {
            None => Ok(None),
            Some(entry) => match entry.value {
                CacheValue::Hash(value) => Ok(Some(value)),
                _ => Err(BackendError::WrongType),
            },
        }
    }

    async fn list_value(&self, key: &str) -> Result<Option<Vec<String>>, BackendError> {
        match self.backend.cache_get(&self.config.cache, key).await? {
            None => Ok(None),
            Some(entry) => match entry.value {
                CacheValue::List(value) => Ok(Some(value)),
                _ => Err(BackendError::WrongType),
            },
        }
    }

    async fn set_value(&self, key: &str) -> Result<Option<Vec<String>>, BackendError> {
        match self.backend.cache_get(&self.config.cache, key).await? {
            None => Ok(None),
            Some(entry) => match entry.value {
                CacheValue::Set(mut value) => {
                    value.sort();
                    value.dedup();
                    Ok(Some(value))
                }
                _ => Err(BackendError::WrongType),
            },
        }
    }

    async fn sorted_set_value(
        &self,
        key: &str,
    ) -> Result<Option<BTreeMap<String, f64>>, BackendError> {
        match self.backend.cache_get(&self.config.cache, key).await? {
            None => Ok(None),
            Some(entry) => match entry.value {
                CacheValue::SortedSet(value) => Ok(Some(value)),
                _ => Err(BackendError::WrongType),
            },
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
        BackendError::Conflict => RespValue::Error("TRYAGAIN Epoch operation conflicted".into()),
        BackendError::WrongType => RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        ),
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

fn bounded_count(value: &[u8]) -> Option<u32> {
    let maximum = u32::try_from(crate::MAX_REQUEST_ITEMS).ok()?;
    text(value)?
        .parse()
        .ok()
        .filter(|count| (1..=maximum).contains(count))
}

fn finite_f64(value: &[u8]) -> Option<f64> {
    text(value)?
        .parse()
        .ok()
        .filter(|value: &f64| value.is_finite())
}

fn utf8_values(values: &[Vec<u8>]) -> Option<Vec<String>> {
    values
        .iter()
        .map(|value| text(value).map(str::to_owned))
        .collect()
}

fn redis_index(length: usize, index: i64) -> Option<usize> {
    let length = i128::try_from(length).ok()?;
    let index = i128::from(index);
    let normalized = if index < 0 { length + index } else { index };
    (0..length)
        .contains(&normalized)
        .then(|| usize::try_from(normalized).ok())
        .flatten()
}

fn redis_range(length: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if length == 0 {
        return None;
    }
    let length = i128::try_from(length).ok()?;
    let normalize = |index: i64| {
        let index = i128::from(index);
        if index < 0 { length + index } else { index }
    };
    let start = normalize(start).max(0);
    let stop = normalize(stop).min(length - 1);
    if start >= length || stop < 0 || start > stop {
        return None;
    }
    Some((
        usize::try_from(start).ok()?,
        usize::try_from(stop + 1).ok()?,
    ))
}

fn count_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn unexpected_collection_result() -> RespValue {
    RespValue::Error("ERR Epoch compatibility backend returned an invalid result".into())
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
    use std::collections::BTreeMap;

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
    async fn set_get_rejects_a_non_string_without_mutating_it() {
        let mut session = session(None);
        let original = CacheValue::Hash(BTreeMap::from([("field".into(), "value".into())]));
        session
            .backend
            .cache_set(
                &session.config.cache,
                "structured",
                original.clone(),
                CacheSetOptions::default(),
            )
            .await
            .unwrap();

        assert!(matches!(
            session
                .execute(command(&[b"SET", b"structured", b"replacement", b"GET"]))
                .await,
            RespValue::Error(message) if message.contains("WRONGTYPE")
        ));
        assert_eq!(
            session
                .backend
                .cache_get(&session.config.cache, "structured")
                .await
                .unwrap()
                .unwrap()
                .value,
            original
        );
    }

    #[tokio::test]
    async fn set_get_returns_the_atomic_previous_value_even_when_condition_fails() {
        let mut session = session(None);
        assert_eq!(
            session.execute(command(&[b"SET", b"key", b"first"])).await,
            RespValue::Simple("OK".into())
        );
        assert_eq!(
            session
                .execute(command(&[b"SET", b"key", b"second", b"NX", b"GET"]))
                .await,
            RespValue::Bulk(b"first".to_vec())
        );
        assert_eq!(
            session.execute(command(&[b"GET", b"key"])).await,
            RespValue::Bulk(b"first".to_vec())
        );
        assert_eq!(
            session
                .execute(command(&[b"SET", b"missing", b"second", b"XX", b"GET"]))
                .await,
            RespValue::Null
        );
        assert_eq!(
            session.execute(command(&[b"GET", b"missing"])).await,
            RespValue::Null
        );
        assert_eq!(
            session
                .execute(command(&[b"SET", b"key", b"second", b"GET"]))
                .await,
            RespValue::Bulk(b"first".to_vec())
        );
        assert_eq!(
            session.execute(command(&[b"GET", b"key"])).await,
            RespValue::Bulk(b"second".to_vec())
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

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one table-like protocol test compares all collection families and ordering rules"
    )]
    async fn executes_atomic_hash_list_set_and_sorted_set_commands() {
        let mut session = session(None);

        assert_eq!(
            session
                .execute(command(&[
                    b"HSET", b"profile", b"name", b"epoch", b"stage", b"beta",
                ]))
                .await,
            RespValue::Integer(2)
        );
        assert_eq!(
            session
                .execute(command(&[b"HSET", b"profile", b"name", b"epoch-db"]))
                .await,
            RespValue::Integer(0)
        );
        assert_eq!(
            session
                .execute(command(&[b"HMGET", b"profile", b"name", b"missing"]))
                .await,
            RespValue::Array(vec![RespValue::Bulk(b"epoch-db".to_vec()), RespValue::Null])
        );
        assert_eq!(
            session.execute(command(&[b"HLEN", b"profile"])).await,
            RespValue::Integer(2)
        );
        assert_eq!(
            session
                .execute(command(&[b"HDEL", b"profile", b"stage", b"missing"]))
                .await,
            RespValue::Integer(1)
        );

        assert_eq!(
            session
                .execute(command(&[b"LPUSH", b"work", b"a", b"b"]))
                .await,
            RespValue::Integer(2)
        );
        assert_eq!(
            session
                .execute(command(&[b"RPUSH", b"work", b"c", b"d"]))
                .await,
            RespValue::Integer(4)
        );
        assert_eq!(
            session
                .execute(command(&[b"LRANGE", b"work", b"0", b"-1"]))
                .await,
            RespValue::Array(
                [b"b", b"a", b"c", b"d"]
                    .into_iter()
                    .map(|value| RespValue::Bulk(value.to_vec()))
                    .collect(),
            )
        );
        assert_eq!(
            session.execute(command(&[b"LPOP", b"work", b"2"])).await,
            RespValue::Array(vec![
                RespValue::Bulk(b"b".to_vec()),
                RespValue::Bulk(b"a".to_vec()),
            ])
        );
        assert_eq!(
            session.execute(command(&[b"RPOP", b"work"])).await,
            RespValue::Bulk(b"d".to_vec())
        );
        assert_eq!(
            session.execute(command(&[b"LPOP", b"work"])).await,
            RespValue::Bulk(b"c".to_vec())
        );
        assert_eq!(
            session.execute(command(&[b"TYPE", b"work"])).await,
            RespValue::Simple("none".into())
        );

        assert_eq!(
            session
                .execute(command(&[b"SADD", b"tags", b"z", b"a", b"z"]))
                .await,
            RespValue::Integer(2)
        );
        assert_eq!(
            session.execute(command(&[b"SMEMBERS", b"tags"])).await,
            RespValue::Array(vec![
                RespValue::Bulk(b"a".to_vec()),
                RespValue::Bulk(b"z".to_vec()),
            ])
        );
        assert_eq!(
            session
                .execute(command(&[b"SREM", b"tags", b"a", b"missing"]))
                .await,
            RespValue::Integer(1)
        );
        assert_eq!(
            session.execute(command(&[b"SCARD", b"tags"])).await,
            RespValue::Integer(1)
        );

        assert_eq!(
            session
                .execute(command(&[
                    b"ZADD", b"scores", b"2", b"b", b"1", b"a", b"1", b"c",
                ]))
                .await,
            RespValue::Integer(3)
        );
        assert_eq!(
            session
                .execute(command(&[b"ZRANGE", b"scores", b"0", b"-1", b"WITHSCORES"]))
                .await,
            RespValue::Array(
                [b"a", b"1", b"c", b"1", b"b", b"2"]
                    .into_iter()
                    .map(|value| RespValue::Bulk(value.to_vec()))
                    .collect(),
            )
        );
        assert_eq!(
            session
                .execute(command(&[b"ZSCORE", b"scores", b"b"]))
                .await,
            RespValue::Bulk(b"2".to_vec())
        );
        assert_eq!(
            session
                .execute(command(&[b"ZREM", b"scores", b"a", b"missing"]))
                .await,
            RespValue::Integer(1)
        );
    }

    #[tokio::test]
    async fn collection_mutations_preserve_ttl_and_fail_wrong_type_without_mutation() {
        let mut session = session(None);
        assert_eq!(
            session
                .execute(command(&[b"HSET", b"expiring", b"a", b"one"]))
                .await,
            RespValue::Integer(1)
        );
        assert_eq!(
            session
                .execute(command(&[b"PEXPIRE", b"expiring", b"5000"]))
                .await,
            RespValue::Integer(1)
        );
        assert_eq!(
            session
                .execute(command(&[b"HSET", b"expiring", b"b", b"two"]))
                .await,
            RespValue::Integer(1)
        );
        assert!(matches!(
            session.execute(command(&[b"PTTL", b"expiring"])).await,
            RespValue::Integer(1..=5_000)
        ));

        assert_eq!(
            session
                .execute(command(&[b"SET", b"scalar", b"safe"]))
                .await,
            RespValue::Simple("OK".into())
        );
        assert!(matches!(
            session
                .execute(command(&[b"SADD", b"scalar", b"corruption"]))
                .await,
            RespValue::Error(message) if message.contains("WRONGTYPE")
        ));
        assert_eq!(
            session.execute(command(&[b"GET", b"scalar"])).await,
            RespValue::Bulk(b"safe".to_vec())
        );
    }
}
