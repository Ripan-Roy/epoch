//! Deterministic MQTT 5 session, retained-message, `QoS`, and shared-route state.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{EpochError, EpochResult, EventEnvelope, validate_resource_name};
use serde::{Deserialize, Serialize};

const MAX_MQTT_SESSIONS: usize = 100_000;
const MAX_MQTT_SUBSCRIPTIONS_PER_SESSION: usize = 1_000;
const MAX_MQTT_RETAINED_MESSAGES: usize = 100_000;
const MAX_MQTT_TOPIC_BYTES: usize = 65_535;
const MAX_MQTT_SESSION_EXPIRY_MS: u64 = 365 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MqttQos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttSubscription {
    pub topic_filter: String,
    pub qos: MqttQos,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_group: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttConnect {
    pub client_id: String,
    pub clean_start: bool,
    pub session_expiry_ms: u64,
    pub connected_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttPublish {
    pub topic: String,
    pub qos: MqttQos,
    pub retain: bool,
    pub envelope: EventEnvelope,
    pub published_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttRetainedMessage {
    pub topic: String,
    pub qos: MqttQos,
    pub envelope: EventEnvelope,
    pub published_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttDelivery {
    pub client_id: String,
    pub topic: String,
    pub qos: MqttQos,
    pub packet_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_group: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttPublishPlan {
    pub packet_sequence: u64,
    pub deliveries: Vec<MqttDelivery>,
    pub retained: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttSession {
    pub client_id: String,
    pub connected: bool,
    pub session_expiry_ms: u64,
    pub last_seen_at_ms: u64,
    #[serde(default)]
    pub subscriptions: BTreeSet<MqttSubscription>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttBrokerState {
    sessions: BTreeMap<String, MqttSession>,
    retained: BTreeMap<String, MqttRetainedMessage>,
    shared_cursors: BTreeMap<String, u64>,
    packet_sequence: u64,
}

impl MqttBrokerState {
    pub fn connect(&mut self, connect: MqttConnect) -> EpochResult<bool> {
        validate_resource_name(&connect.client_id)?;
        validate_session_expiry(connect.session_expiry_ms)?;
        if !self.sessions.contains_key(&connect.client_id)
            && self.sessions.len() >= MAX_MQTT_SESSIONS
        {
            return Err(EpochError::Capacity(format!(
                "MQTT broker reached its {MAX_MQTT_SESSIONS} session limit"
            )));
        }
        let resumed = !connect.clean_start && self.sessions.contains_key(&connect.client_id);
        let subscriptions = if resumed {
            self.sessions
                .get(&connect.client_id)
                .map(|session| session.subscriptions.clone())
                .unwrap_or_default()
        } else {
            BTreeSet::new()
        };
        self.sessions.insert(
            connect.client_id.clone(),
            MqttSession {
                client_id: connect.client_id,
                connected: true,
                session_expiry_ms: connect.session_expiry_ms,
                last_seen_at_ms: connect.connected_at_ms,
                subscriptions,
            },
        );
        Ok(resumed)
    }

    pub fn disconnect(&mut self, client_id: &str, disconnected_at_ms: u64) -> EpochResult<()> {
        validate_resource_name(client_id)?;
        let remove = {
            let session = self
                .sessions
                .get_mut(client_id)
                .ok_or_else(|| EpochError::NotFound(client_id.to_owned()))?;
            session.connected = false;
            session.last_seen_at_ms = disconnected_at_ms;
            session.session_expiry_ms == 0
        };
        if remove {
            self.sessions.remove(client_id);
        }
        Ok(())
    }

    pub fn subscribe(
        &mut self,
        client_id: &str,
        subscription: MqttSubscription,
    ) -> EpochResult<Vec<MqttRetainedMessage>> {
        validate_resource_name(client_id)?;
        validate_subscription(&subscription)?;
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or_else(|| EpochError::NotFound(client_id.to_owned()))?;
        if !session.connected {
            return Err(EpochError::Unavailable(format!(
                "MQTT session {client_id} is disconnected"
            )));
        }
        if !session.subscriptions.contains(&subscription)
            && session.subscriptions.len() >= MAX_MQTT_SUBSCRIPTIONS_PER_SESSION
        {
            return Err(EpochError::Capacity(format!(
                "MQTT session reached its {MAX_MQTT_SUBSCRIPTIONS_PER_SESSION} subscription limit"
            )));
        }
        let retained = self
            .retained
            .values()
            .filter(|message| topic_matches(&subscription.topic_filter, &message.topic))
            .cloned()
            .collect();
        session.subscriptions.insert(subscription);
        Ok(retained)
    }

    pub fn unsubscribe(
        &mut self,
        client_id: &str,
        subscription: &MqttSubscription,
    ) -> EpochResult<bool> {
        validate_resource_name(client_id)?;
        validate_subscription(subscription)?;
        self.sessions
            .get_mut(client_id)
            .map(|session| session.subscriptions.remove(subscription))
            .ok_or_else(|| EpochError::NotFound(client_id.to_owned()))
    }

    pub fn publish(&mut self, publish: MqttPublish) -> EpochResult<MqttPublishPlan> {
        validate_topic_name(&publish.topic)?;
        publish.envelope.validate()?;
        if publish.retain
            && !self.retained.contains_key(&publish.topic)
            && self.retained.len() >= MAX_MQTT_RETAINED_MESSAGES
        {
            return Err(EpochError::Capacity(format!(
                "MQTT retained store reached its {MAX_MQTT_RETAINED_MESSAGES} message limit"
            )));
        }
        let next_sequence = self
            .packet_sequence
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("MQTT packet sequence overflow".into()))?;

        let mut direct = BTreeMap::<String, MqttQos>::new();
        let mut shared = BTreeMap::<(String, String), Vec<(String, MqttQos)>>::new();
        for (client_id, session) in &self.sessions {
            if !session.connected {
                continue;
            }
            for subscription in &session.subscriptions {
                if !topic_matches(&subscription.topic_filter, &publish.topic) {
                    continue;
                }
                let qos = minimum_qos(publish.qos, subscription.qos);
                if let Some(group) = &subscription.shared_group {
                    shared
                        .entry((group.clone(), subscription.topic_filter.clone()))
                        .or_default()
                        .push((client_id.clone(), qos));
                } else {
                    direct
                        .entry(client_id.clone())
                        .and_modify(|current| *current = (*current).max(qos))
                        .or_insert(qos);
                }
            }
        }

        let mut deliveries = direct
            .into_iter()
            .map(|(client_id, qos)| MqttDelivery {
                client_id,
                topic: publish.topic.clone(),
                qos,
                packet_sequence: next_sequence,
                shared_group: None,
            })
            .collect::<Vec<_>>();
        for ((group, filter), mut candidates) in shared {
            candidates.sort_by(|left, right| left.0.cmp(&right.0));
            let cursor_key = format!("{group}\0{filter}");
            let cursor = self.shared_cursors.entry(cursor_key).or_default();
            let candidate_count = u64::try_from(candidates.len())
                .map_err(|_| EpochError::Capacity("MQTT shared member count overflow".into()))?;
            let index = usize::try_from(*cursor % candidate_count)
                .map_err(|_| EpochError::Capacity("MQTT shared cursor overflow".into()))?;
            let (client_id, qos) = candidates[index].clone();
            *cursor = cursor
                .checked_add(1)
                .ok_or_else(|| EpochError::Capacity("MQTT shared cursor overflow".into()))?;
            deliveries.push(MqttDelivery {
                client_id,
                topic: publish.topic.clone(),
                qos,
                packet_sequence: next_sequence,
                shared_group: Some(group),
            });
        }
        deliveries.sort_by(|left, right| {
            (&left.client_id, &left.shared_group).cmp(&(&right.client_id, &right.shared_group))
        });

        if publish.retain {
            self.retained.insert(
                publish.topic.clone(),
                MqttRetainedMessage {
                    topic: publish.topic,
                    qos: publish.qos,
                    envelope: publish.envelope,
                    published_at_ms: publish.published_at_ms,
                },
            );
        }
        self.packet_sequence = next_sequence;
        Ok(MqttPublishPlan {
            packet_sequence: next_sequence,
            deliveries,
            retained: publish.retain,
        })
    }

    pub fn clear_retained(&mut self, topic: &str) -> EpochResult<bool> {
        validate_topic_name(topic)?;
        Ok(self.retained.remove(topic).is_some())
    }

    pub fn expire_sessions(&mut self, now_ms: u64, limit: usize) -> EpochResult<usize> {
        if limit == 0 || limit > MAX_MQTT_SESSIONS {
            return Err(EpochError::InvalidArgument(format!(
                "MQTT expiry limit must be between 1 and {MAX_MQTT_SESSIONS}"
            )));
        }
        let expired = self
            .sessions
            .values()
            .filter(|session| {
                !session.connected
                    && session
                        .last_seen_at_ms
                        .checked_add(session.session_expiry_ms)
                        .is_none_or(|deadline| deadline <= now_ms)
            })
            .map(|session| session.client_id.clone())
            .take(limit)
            .collect::<Vec<_>>();
        for client_id in &expired {
            self.sessions.remove(client_id);
        }
        Ok(expired.len())
    }

    pub fn retained(&self, topic: &str) -> Option<&MqttRetainedMessage> {
        self.retained.get(topic)
    }

    pub fn session(&self, client_id: &str) -> Option<&MqttSession> {
        self.sessions.get(client_id)
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
            && self.retained.is_empty()
            && self.shared_cursors.is_empty()
            && self.packet_sequence == 0
    }

    pub(crate) fn validate_snapshot(&self) -> EpochResult<()> {
        if self.sessions.len() > MAX_MQTT_SESSIONS
            || self.retained.len() > MAX_MQTT_RETAINED_MESSAGES
        {
            return Err(EpochError::InvalidArgument(
                "MQTT snapshot exceeds its configured state limits".into(),
            ));
        }
        for (client_id, session) in &self.sessions {
            validate_resource_name(client_id)?;
            validate_session_expiry(session.session_expiry_ms)?;
            if session.client_id != *client_id
                || session.subscriptions.len() > MAX_MQTT_SUBSCRIPTIONS_PER_SESSION
            {
                return Err(EpochError::InvalidArgument(format!(
                    "MQTT session {client_id} snapshot is invalid"
                )));
            }
            for subscription in &session.subscriptions {
                validate_subscription(subscription)?;
            }
        }
        for (topic, retained) in &self.retained {
            validate_topic_name(topic)?;
            retained.envelope.validate()?;
            if retained.topic != *topic || self.packet_sequence == 0 {
                return Err(EpochError::InvalidArgument(format!(
                    "MQTT retained topic {topic} snapshot is invalid"
                )));
            }
        }
        for (key, cursor) in &self.shared_cursors {
            let Some((group, filter)) = key.split_once('\0') else {
                return Err(EpochError::InvalidArgument(
                    "MQTT shared cursor key is invalid".into(),
                ));
            };
            validate_resource_name(group)?;
            validate_topic_filter(filter)?;
            if *cursor == 0 || *cursor > self.packet_sequence || filter.contains('\0') {
                return Err(EpochError::InvalidArgument(
                    "MQTT shared cursor position is invalid".into(),
                ));
            }
        }
        Ok(())
    }
}

fn minimum_qos(left: MqttQos, right: MqttQos) -> MqttQos {
    if left < right { left } else { right }
}

fn validate_session_expiry(value: u64) -> EpochResult<()> {
    if value > MAX_MQTT_SESSION_EXPIRY_MS {
        return Err(EpochError::InvalidArgument(format!(
            "MQTT session expiry cannot exceed {MAX_MQTT_SESSION_EXPIRY_MS} ms"
        )));
    }
    Ok(())
}

fn validate_subscription(subscription: &MqttSubscription) -> EpochResult<()> {
    validate_topic_filter(&subscription.topic_filter)?;
    if let Some(group) = &subscription.shared_group {
        validate_resource_name(group)?;
    }
    Ok(())
}

fn validate_topic_name(topic: &str) -> EpochResult<()> {
    validate_topic_text(topic)?;
    if topic.contains(['+', '#']) {
        return Err(EpochError::InvalidArgument(
            "MQTT publish topics cannot contain wildcards".into(),
        ));
    }
    Ok(())
}

fn validate_topic_filter(filter: &str) -> EpochResult<()> {
    validate_topic_text(filter)?;
    let levels = filter.split('/').collect::<Vec<_>>();
    for (index, level) in levels.iter().enumerate() {
        if level.contains('#') && (*level != "#" || index + 1 != levels.len()) {
            return Err(EpochError::InvalidArgument(
                "MQTT # wildcard must occupy the final topic level".into(),
            ));
        }
        if level.contains('+') && *level != "+" {
            return Err(EpochError::InvalidArgument(
                "MQTT + wildcard must occupy one complete topic level".into(),
            ));
        }
    }
    Ok(())
}

fn validate_topic_text(topic: &str) -> EpochResult<()> {
    if topic.is_empty()
        || topic.len() > MAX_MQTT_TOPIC_BYTES
        || topic.contains('\0')
        || topic.chars().any(char::is_control)
    {
        return Err(EpochError::InvalidArgument(format!(
            "MQTT topic must be between 1 and {MAX_MQTT_TOPIC_BYTES} printable bytes"
        )));
    }
    Ok(())
}

fn topic_matches(filter: &str, topic: &str) -> bool {
    let filter_levels = filter.split('/').collect::<Vec<_>>();
    let topic_levels = topic.split('/').collect::<Vec<_>>();
    let mut topic_index = 0;
    for level in filter_levels {
        if level == "#" {
            return true;
        }
        let Some(topic_level) = topic_levels.get(topic_index) else {
            return false;
        };
        if level != "+" && level != *topic_level {
            return false;
        }
        topic_index += 1;
    }
    topic_index == topic_levels.len()
}
