//! Redis Pub/Sub session state backed by Epoch's node-local Cache hub.

use std::{collections::BTreeSet, sync::Arc};

use crate::{BackendError, CompatibilityBackend};

use super::{RespValue, arity, backend_error, bulk, error, integer, text};

const POLL_LIMIT: u16 = 100;
const MAX_FILTERS: usize = 64;

#[derive(Debug, Default)]
pub(super) struct PubSubState {
    channels: BTreeSet<String>,
    patterns: BTreeSet<String>,
    subscription_id: Option<String>,
}

impl PubSubState {
    pub(super) fn active(&self) -> bool {
        self.subscription_id.is_some()
    }

    pub(super) async fn subscribe<B: CompatibilityBackend>(
        &mut self,
        backend: &Arc<B>,
        cache: &str,
        arguments: &[Vec<u8>],
        pattern: bool,
        resp3: bool,
    ) -> Vec<RespValue> {
        if arguments.is_empty() {
            return vec![arity(if pattern { "psubscribe" } else { "subscribe" })];
        }
        let mut channels = self.channels.clone();
        let mut patterns = self.patterns.clone();
        let mut names = Vec::with_capacity(arguments.len());
        for argument in arguments {
            let Some(name) = text(argument).filter(|name| !name.is_empty()) else {
                return vec![error(
                    "Pub/Sub channels and patterns must be non-empty UTF-8",
                )];
            };
            names.push(name.to_owned());
            if pattern {
                patterns.insert(name.to_owned());
            } else {
                channels.insert(name.to_owned());
            }
        }
        if channels.len().saturating_add(patterns.len()) > MAX_FILTERS {
            return vec![error("Pub/Sub subscription filter limit exceeded")];
        }
        if let Err(error) = self
            .replace_subscription(backend, cache, channels, patterns)
            .await
        {
            return vec![backend_error(error)];
        }
        names
            .into_iter()
            .map(|name| {
                notification(
                    if pattern { "psubscribe" } else { "subscribe" },
                    Some(name.as_bytes()),
                    self.filter_count(),
                    resp3,
                )
            })
            .collect()
    }

    pub(super) async fn unsubscribe<B: CompatibilityBackend>(
        &mut self,
        backend: &Arc<B>,
        cache: &str,
        arguments: &[Vec<u8>],
        pattern: bool,
        resp3: bool,
    ) -> Vec<RespValue> {
        let existing = if pattern {
            self.patterns.clone()
        } else {
            self.channels.clone()
        };
        let names = if arguments.is_empty() {
            if existing.is_empty() {
                vec![None]
            } else {
                existing.iter().map(|name| Some(name.clone())).collect()
            }
        } else {
            let mut names = Vec::with_capacity(arguments.len());
            for argument in arguments {
                let Some(name) = text(argument).filter(|name| !name.is_empty()) else {
                    return vec![error(
                        "Pub/Sub channels and patterns must be non-empty UTF-8",
                    )];
                };
                names.push(Some(name.to_owned()));
            }
            names
        };
        let mut channels = self.channels.clone();
        let mut patterns = self.patterns.clone();
        for name in names.iter().flatten() {
            if pattern {
                patterns.remove(name);
            } else {
                channels.remove(name);
            }
        }
        if let Err(error) = self
            .replace_subscription(backend, cache, channels, patterns)
            .await
        {
            return vec![backend_error(error)];
        }
        let mut remaining = existing.len();
        names
            .into_iter()
            .map(|name| {
                if name.as_ref().is_some_and(|name| existing.contains(name)) {
                    remaining = remaining.saturating_sub(1);
                }
                let other = if pattern {
                    self.channels.len()
                } else {
                    self.patterns.len()
                };
                notification(
                    if pattern {
                        "punsubscribe"
                    } else {
                        "unsubscribe"
                    },
                    name.as_deref().map(str::as_bytes),
                    remaining.saturating_add(other),
                    resp3,
                )
            })
            .collect()
    }

    pub(super) async fn publish<B: CompatibilityBackend>(
        backend: &Arc<B>,
        cache: &str,
        arguments: &[Vec<u8>],
    ) -> RespValue {
        let [channel, payload] = arguments else {
            return arity("publish");
        };
        let Some(channel) = text(channel).filter(|channel| !channel.is_empty()) else {
            return error("Pub/Sub channel must be non-empty UTF-8");
        };
        match backend.cache_pubsub_publish(cache, channel, payload).await {
            Ok(delivered) => integer(delivered),
            Err(error) => backend_error(error),
        }
    }

    pub(super) async fn poll<B: CompatibilityBackend>(
        &self,
        backend: &Arc<B>,
        cache: &str,
        resp3: bool,
    ) -> Vec<RespValue> {
        let Some(subscription_id) = self.subscription_id.as_deref() else {
            return Vec::new();
        };
        match backend
            .cache_pubsub_poll(cache, subscription_id, POLL_LIMIT)
            .await
        {
            Ok(messages) => messages
                .into_iter()
                .flat_map(|message| {
                    let mut responses = Vec::new();
                    if self.channels.contains(&message.channel) {
                        responses.push(push(
                            vec![
                                bulk("message"),
                                bulk(&message.channel),
                                RespValue::Bulk(message.payload.clone()),
                            ],
                            resp3,
                        ));
                    }
                    for pattern in self.patterns.iter().filter(|pattern| {
                        glob_matches(pattern.as_bytes(), message.channel.as_bytes())
                    }) {
                        responses.push(push(
                            vec![
                                bulk("pmessage"),
                                bulk(pattern),
                                bulk(&message.channel),
                                RespValue::Bulk(message.payload.clone()),
                            ],
                            resp3,
                        ));
                    }
                    responses
                })
                .collect(),
            Err(BackendError::NotFound) => Vec::new(),
            Err(error) => vec![backend_error(error)],
        }
    }

    pub(super) async fn close<B: CompatibilityBackend>(&mut self, backend: &Arc<B>, cache: &str) {
        if let Some(subscription_id) = self.subscription_id.take() {
            let _ = backend
                .cache_pubsub_unsubscribe(cache, &subscription_id)
                .await;
        }
        self.channels.clear();
        self.patterns.clear();
    }

    async fn replace_subscription<B: CompatibilityBackend>(
        &mut self,
        backend: &Arc<B>,
        cache: &str,
        channels: BTreeSet<String>,
        patterns: BTreeSet<String>,
    ) -> Result<(), BackendError> {
        if let Some(subscription_id) = self.subscription_id.as_deref() {
            backend
                .cache_pubsub_unsubscribe(cache, subscription_id)
                .await?;
        }
        self.subscription_id = None;
        if !channels.is_empty() || !patterns.is_empty() {
            self.subscription_id = Some(
                backend
                    .cache_pubsub_subscribe(
                        cache,
                        &channels.iter().cloned().collect::<Vec<_>>(),
                        &patterns.iter().cloned().collect::<Vec<_>>(),
                    )
                    .await?,
            );
        }
        self.channels = channels;
        self.patterns = patterns;
        Ok(())
    }

    fn filter_count(&self) -> usize {
        self.channels.len().saturating_add(self.patterns.len())
    }
}

fn notification(kind: &str, name: Option<&[u8]>, count: usize, resp3: bool) -> RespValue {
    push(
        vec![
            bulk(kind),
            name.map_or(RespValue::Null, |name| RespValue::Bulk(name.to_vec())),
            RespValue::Integer(i64::try_from(count).unwrap_or(i64::MAX)),
        ],
        resp3,
    )
}

fn push(values: Vec<RespValue>, resp3: bool) -> RespValue {
    if resp3 {
        RespValue::Push(values)
    } else {
        RespValue::Array(values)
    }
}

fn glob_matches(pattern: &[u8], value: &[u8]) -> bool {
    let (mut pattern_index, mut value_index) = (0, 0);
    let (mut star_index, mut star_value_index) = (None, 0);
    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            star_value_index = value_index;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}
