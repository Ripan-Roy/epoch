import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;

public final class ProtocolConformance {
  private ProtocolConformance() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 3) {
      throw new IllegalArgumentException("expected Kafka host, Kafka port, and AMQP port");
    }
    var host = arguments[0];
    var kafkaPort = Integer.parseInt(arguments[1]);
    var amqpPort = Integer.parseInt(arguments[2]);
    require(AppInfoParser.getVersion().equals("4.3.1"), "Kafka client version");
    require(
        ConnectionFactory.class.getPackage().getImplementationVersion().equals("5.34.0"),
        "RabbitMQ client version");
    verifyKafka(host, kafkaPort);
    verifyAmqp(host, amqpPort);
    System.out.println("Kafka 4.3.1 and RabbitMQ Java 5.34.0 conformance passed");
  }

  private static void verifyKafka(String host, int port) throws Exception {
    var bootstrap = host + ":" + port;
    var producerProperties = new HashMap<String, Object>();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "epoch-conformance-producer");
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");
    producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
    producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
    try (var producer = new KafkaProducer<byte[], byte[]>(producerProperties)) {
      var metadata =
          producer
              .send(
                  new ProducerRecord<>(
                      "events",
                      1,
                      "compat-key".getBytes(StandardCharsets.UTF_8),
                      "kafka-value".getBytes(StandardCharsets.UTF_8)))
              .get(10, TimeUnit.SECONDS);
      require(metadata.partition() == 1 && metadata.offset() == 0, "Kafka produce receipt");
    }

    var consumerProperties = new HashMap<String, Object>();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "epoch-conformance-consumer");
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "billing");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
    var partition = new TopicPartition("events", 1);
    try (var consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties)) {
      consumer.assign(List.of(partition));
      consumer.seek(partition, 0);
      var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      var observed = false;
      while (!observed && System.nanoTime() < deadline) {
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          if (record.partition() == 1
              && record.offset() == 0
              && new String(record.value(), StandardCharsets.UTF_8).equals("kafka-value")) {
            observed = true;
          }
        }
      }
      require(observed, "Kafka manual fetch");
      consumer.commitSync(Map.of(partition, new OffsetAndMetadata(1)));
      require(
          consumer.committed(Set.of(partition)).get(partition).offset() == 1,
          "Kafka durable offset");
    }
  }

  private static void verifyAmqp(String host, int port) throws Exception {
    var factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername("epoch");
    factory.setPassword("compat-secret");
    factory.setVirtualHost("/");
    factory.setAutomaticRecoveryEnabled(false);
    factory.setConnectionTimeout(5_000);
    factory.setHandshakeTimeout(5_000);
    try (var connection = factory.newConnection("epoch-conformance");
        var channel = connection.createChannel()) {
      channel.queueDeclare("jobs", true, false, false, Map.of());
      channel.confirmSelect();
      channel.basicPublish(
          "", "jobs", null, "rabbit-pull".getBytes(StandardCharsets.UTF_8));
      channel.waitForConfirmsOrDie(5_000);
      var pulled = channel.basicGet("jobs", false);
      require(pulled != null, "AMQP basic.get delivery");
      require(
          new String(pulled.getBody(), StandardCharsets.UTF_8).equals("rabbit-pull"),
          "AMQP binary body");
      channel.basicAck(pulled.getEnvelope().getDeliveryTag(), false);

      channel.basicQos(1);
      var consumed = new CountDownLatch(1);
      var consumerTag =
          channel.basicConsume(
              "jobs",
              false,
              (tag, delivery) -> {
                if (new String(delivery.getBody(), StandardCharsets.UTF_8)
                    .equals("rabbit-push")) {
                  channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                  consumed.countDown();
                }
              },
              tag -> {});
      channel.basicPublish(
          "", "jobs", null, "rabbit-push".getBytes(StandardCharsets.UTF_8));
      channel.waitForConfirmsOrDie(5_000);
      require(consumed.await(10, TimeUnit.SECONDS), "AMQP push delivery");
      channel.basicCancel(consumerTag);
    }
  }

  private static void require(boolean condition, String evidence) {
    if (!condition) {
      throw new AssertionError(evidence + " did not match");
    }
  }
}
