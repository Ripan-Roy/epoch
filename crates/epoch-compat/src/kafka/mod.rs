//! Kafka broker-protocol compatibility gateway.

mod server;

pub use server::{KafkaConfig, KafkaServer, SUPPORTED_APIS};
