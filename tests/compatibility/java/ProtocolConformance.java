import com.rabbitmq.client.AMQP;
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
        ConnectionFactory.class.getPackage().getImplementationVersion().equals("5.35.0"),
        "RabbitMQ client version");
    verifyKafka(host, kafkaPort);
    verifyAmqp(host, amqpPort);
    System.out.println("Kafka 4.3.1 and RabbitMQ Java 5.35.0 conformance passed");
  }

  private static void verifyKafka(String host, int port) throws Exception {
    var bootstrap = host + ":" + port;
    var producerProperties = new HashMap<String, Object>();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "epoch-conformance-producer");
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
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

    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-subscribe");
    consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "epoch-conformance-group-consumer");
    try (var consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties)) {
      consumer.subscribe(List.of("events"));
      var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
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
      require(observed, "Kafka native-backed consumer group subscribe");
      consumer.commitSync();
      require(!consumer.assignment().isEmpty(), "Kafka consumer group assignment");
    }

    var staticConsumerProperties = new HashMap<String, Object>(consumerProperties);
    staticConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-static");
    staticConsumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "epoch-static-consumer");
    staticConsumerProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "billing-worker-a");
    try (var consumer = new KafkaConsumer<byte[], byte[]>(staticConsumerProperties)) {
      consumer.subscribe(List.of("events"));
      var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
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
      require(observed, "Kafka static group member subscription");
      consumer.commitSync();
      require(!consumer.assignment().isEmpty(), "Kafka static member assignment");
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
      channel.exchangeDeclare("epoch.dead.topic", "topic", true, false, Map.of());
      channel.queueDeclare("failed-jobs", true, false, false, Map.of());
      channel.queueBind("failed-jobs", "epoch.dead.topic", "failed.#");
      var deadLetterArguments =
          Map.<String, Object>of(
              "x-dead-letter-exchange", "epoch.dead.topic",
              "x-dead-letter-routing-key", "failed.jobs");
      channel.queueDeclare("audit", true, false, false, Map.of());
      channel.queueDeclare("jobs", true, false, false, deadLetterArguments);
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

      channel.exchangeDeclare("epoch.events.topic", "topic", false, false, Map.of());
      channel.queueBind("jobs", "epoch.events.topic", "orders.*");
      var routedProperties =
          new AMQP.BasicProperties.Builder()
              .expiration("5000")
              .headers(Map.of("tenant", "acme"))
              .build();
      channel.basicPublish(
          "epoch.events.topic",
          "orders.created",
          true,
          routedProperties,
          "rabbit-topic".getBytes(StandardCharsets.UTF_8));
      channel.waitForConfirmsOrDie(5_000);
      var routed = channel.basicGet("jobs", true);
      require(routed != null, "AMQP topic delivery");
      require(
          routed.getEnvelope().getExchange().equals("epoch.events.topic")
              && routed.getEnvelope().getRoutingKey().equals("orders.created"),
          "AMQP original routing metadata");
      require(
          "5000".equals(routed.getProps().getExpiration())
              && "acme".equals(routed.getProps().getHeaders().get("tenant").toString()),
          "AMQP expiration and string headers");

      var returned = new CountDownLatch(1);
      channel.addReturnListener(
          message -> {
            if (message.getReplyCode() == 312
                && message.getExchange().equals("epoch.events.topic")
                && new String(message.getBody(), StandardCharsets.UTF_8)
                    .equals("rabbit-unroutable")) {
              returned.countDown();
            }
          });
      channel.basicPublish(
          "epoch.events.topic",
          "payments.created",
          true,
          null,
          "rabbit-unroutable".getBytes(StandardCharsets.UTF_8));
      channel.waitForConfirmsOrDie(5_000);
      require(returned.await(5, TimeUnit.SECONDS), "AMQP mandatory basic.return");
      channel.queueUnbind("jobs", "epoch.events.topic", "orders.*");
      channel.exchangeDelete("epoch.events.topic");

      channel.exchangeDeclare("epoch.events.headers", "headers", false, false, Map.of());
      var allHeaders =
          Map.<String, Object>of("x-match", "all", "tenant", "acme", "format", "json");
      var anyHeaders =
          Map.<String, Object>of("x-match", "any", "tenant", "acme", "priority", "high");
      channel.queueBind("jobs", "epoch.events.headers", "", allHeaders);
      channel.queueBind("audit", "epoch.events.headers", "", anyHeaders);
      var headerProperties =
          new AMQP.BasicProperties.Builder()
              .headers(Map.of("tenant", "acme", "format", "json"))
              .build();
      channel.basicPublish(
          "epoch.events.headers",
          "ignored",
          true,
          headerProperties,
          "rabbit-headers".getBytes(StandardCharsets.UTF_8));
      channel.waitForConfirmsOrDie(5_000);
      for (var queue : List.of("jobs", "audit")) {
        var headerRouted = channel.basicGet(queue, true);
        require(
            headerRouted != null
                && new String(headerRouted.getBody(), StandardCharsets.UTF_8)
                    .equals("rabbit-headers"),
            "AMQP headers x-match routing: " + queue);
      }
      channel.queueUnbind("jobs", "epoch.events.headers", "", allHeaders);
      channel.queueUnbind("audit", "epoch.events.headers", "", anyHeaders);
      channel.exchangeDelete("epoch.events.headers");

      var expiring = new AMQP.BasicProperties.Builder().expiration("5000").build();
      channel.basicPublish(
          "", "jobs", expiring, "rabbit-poison".getBytes(StandardCharsets.UTF_8));
      channel.waitForConfirmsOrDie(5_000);
      var poison = channel.basicGet("jobs", false);
      require(poison != null, "AMQP dead-letter source delivery");
      channel.basicReject(poison.getEnvelope().getDeliveryTag(), false);
      channel.queueDeclarePassive("jobs");
      var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      var deadLetter = channel.basicGet("failed-jobs", true);
      while (deadLetter == null && System.nanoTime() < deadline) {
        Thread.sleep(100);
        deadLetter = channel.basicGet("failed-jobs", true);
      }
      require(deadLetter != null, "AMQP native dead-letter forwarding");
      require(
          deadLetter.getEnvelope().getExchange().equals("epoch.dead.topic")
              && deadLetter.getEnvelope().getRoutingKey().equals("failed.jobs"),
          "AMQP named dead-letter route");
      require(
          deadLetter.getProps().getHeaders().get("x-death") instanceof List<?> deaths
              && !deaths.isEmpty(),
          "AMQP x-death history");
      require(
          new String(deadLetter.getBody(), StandardCharsets.UTF_8).equals("rabbit-poison"),
          "AMQP dead-letter body");
      require(
          deadLetter.getProps().getExpiration() == null,
          "AMQP dead-letter expiration removal");
      require(
          "jobs".equals(deadLetter.getProps().getHeaders().get("x-first-death-queue").toString())
              && "rejected"
                  .equals(
                      deadLetter
                          .getProps()
                          .getHeaders()
                          .get("x-first-death-reason")
                          .toString()),
          "AMQP first-death metadata");
    }
  }

  private static void require(boolean condition, String evidence) {
    if (!condition) {
      throw new AssertionError(evidence + " did not match");
    }
  }
}
