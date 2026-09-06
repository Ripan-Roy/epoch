//! Kafka broker-protocol compatibility gateway.

mod records;
mod server;

pub use server::{KafkaConfig, KafkaServer, SUPPORTED_APIS};
