import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

/** Released clients verifying persisted state across externally injected faults. */
public final class RegionalRecoveryConformance {
  private static final TopicPartition PARTITION = new TopicPartition("events", 0);
  private static final List<String> CODECS = List.of("gzip", "snappy", "lz4", "zstd");
  private static final byte[] BODY = new byte[] {0, 1, 2, (byte) 255};
  private static final long TIMESTAMP = 1_700_000_000_000L;
  private static final String DEAD_EXCHANGE = "epoch.dead.topic";
  private static final String DEAD_QUEUE = "failed-jobs";
  private static final String DEAD_SOURCE = "dead-letter-source";

  private RegionalRecoveryConformance() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 4) {
      throw new IllegalArgumentException("expected host, Kafka port, AMQP port, phase");
    }
    var bootstrap = arguments[0] + ":" + arguments[1];
    var phase = arguments[3];
    if (phase.equals("capacity")) {
      verifyCapacityRejection(arguments[0], Integer.parseInt(arguments[2]));
      System.out.println("Native capacity rejection was not publisher-confirmed");
      return;
    }
    if (!List.of("seed", "verify", "settle", "empty").contains(phase)) {
      throw new IllegalArgumentException("unknown recovery phase: " + phase);
    }
    if (phase.equals("seed")) {
      seedKafka(bootstrap);
    } else {
      verifyKafka(bootstrap);
    }
    var factory = new ConnectionFactory();
    factory.setHost(arguments[0]);
    factory.setPort(Integer.parseInt(arguments[2]));
    factory.setUsername("epoch");
    factory.setPassword("compat-secret");
    factory.setAutomaticRecoveryEnabled(false);
    factory.setConnectionTimeout(5_000);
    factory.setHandshakeTimeout(5_000);
    try (var connection = factory.newConnection("epoch-regional-recovery");
        var channel = connection.createChannel()) {
      configureAmqpTopology(channel, phase.equals("seed"));
      channel.confirmSelect();
      switch (phase) {
        case "seed" -> seedAmqp(channel);
        case "verify" -> verifyAmqp(channel);
        case "settle" -> settleAmqp(channel);
        case "empty" -> {
          require(channel.basicGet("jobs", false) == null, "acknowledged job stayed absent");
          require(channel.basicGet("leases", false) == null, "acknowledged lease stayed absent");
          require(channel.basicGet(DEAD_SOURCE, false) == null, "dead-letter source stayed absent");
          require(channel.basicGet(DEAD_QUEUE, false) == null, "dead-letter target stayed absent");
        }
        default -> throw new AssertionError("unreachable phase");
      }
    }
    System.out.println("Regional Kafka/AMQP recovery phase passed: " + phase);
  }

  private static Map<String, Object> producerProperties(String bootstrap, String codec) {
    var properties = new HashMap<String, Object>();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    // Retries retain the broker-issued producer identity and sequence, so the
    // recovery campaign exercises exact replay instead of duplicating writes.
    properties.put(ProducerConfig.RETRIES_CONFIG, 5);
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
    properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
    return properties;
  }

  private static KafkaConsumer<byte[], byte[]> consumer(String bootstrap) {
    var properties = new HashMap<String, Object>();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "recovery");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
    return new KafkaConsumer<>(properties);
  }

  private static KafkaConsumer<byte[], byte[]> staticConsumer(String bootstrap) {
    var properties = new HashMap<String, Object>();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "recovery-static");
    properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "recovery-worker-a");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
    return new KafkaConsumer<>(properties);
  }

  private static byte[] key(int index) {
    return index == 1 ? null : ("key-" + index).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] value(int index) {
    return index == 2 ? null : BODY;
  }

  private static void seedKafka(String bootstrap) throws Exception {
    for (var index = 0; index < CODECS.size(); index++) {
      var codec = CODECS.get(index);
      try (var producer = new KafkaProducer<byte[], byte[]>(producerProperties(bootstrap, codec))) {
        var record = new ProducerRecord<byte[], byte[]>(
            "events", 0, TIMESTAMP + index, key(index), value(index));
        record.headers().add("codec", codec.getBytes(StandardCharsets.UTF_8));
        record.headers().add("nullable", null);
        record.headers().add("codec", BODY);
        var receipt = producer.send(record).get(10, TimeUnit.SECONDS);
        require(receipt.partition() == 0 && receipt.offset() == index, "atomic compressed append");
      }
    }
    try (var consumer = consumer(bootstrap)) {
      consumer.assign(List.of(PARTITION));
      consumer.commitSync(Map.of(PARTITION, new OffsetAndMetadata(2)));
    }
    verifyKafka(bootstrap);
  }

  private static void verifyKafka(String bootstrap) {
    try (var consumer = consumer(bootstrap)) {
      consumer.assign(List.of(PARTITION));
      var checkpoint = consumer.committed(Set.of(PARTITION)).get(PARTITION);
      require(checkpoint != null && checkpoint.offset() == 2, "persisted Kafka checkpoint");
      require(consumer.beginningOffsets(List.of(PARTITION)).get(PARTITION) == 0, "start offset");
      require(consumer.endOffsets(List.of(PARTITION)).get(PARTITION) == 4, "no missing/extra writes");
      consumer.seek(PARTITION, 0);
      var count = 0;
      var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while (count < CODECS.size() && System.nanoTime() < deadline) {
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          require(count < CODECS.size() && record.offset() == count, "exact ordered Kafka history");
          require(Arrays.equals(record.key(), key(count)), "nullable/binary key");
          require(Arrays.equals(record.value(), value(count)), "nullable/binary value");
          require(record.timestamp() == TIMESTAMP + count, "timestamp round trip");
          var headers = record.headers().toArray();
          require(headers.length == 3, "duplicate Kafka headers preserved");
          require(headers[0].key().equals("codec") && Arrays.equals(headers[0].value(),
              CODECS.get(count).getBytes(StandardCharsets.UTF_8)), "codec header");
          require(headers[1].key().equals("nullable") && headers[1].value() == null, "null header");
          require(headers[2].key().equals("codec") && Arrays.equals(headers[2].value(), BODY),
              "duplicate header order and value");
          count++;
        }
      }
      require(count == CODECS.size(), "complete persisted Kafka history");
    }
    try (var consumer = staticConsumer(bootstrap)) {
      consumer.subscribe(List.of("events"));
      var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
        consumer.poll(Duration.ofMillis(250));
      }
      require(!consumer.assignment().isEmpty(), "static member assignment survives recovery");
    }
  }

  private static void publish(Channel channel, String queue) throws Exception {
    var properties = new AMQP.BasicProperties.Builder()
        .contentType("application/octet-stream").correlationId("recovery-42")
        .replyTo("reply-queue").build();
    channel.basicPublish("", queue, properties, BODY);
    channel.waitForConfirmsOrDie(5_000);
  }

  private static void configureAmqpTopology(Channel channel, boolean create) throws Exception {
    if (create) {
      channel.exchangeDeclare(DEAD_EXCHANGE, "topic", true, false, Map.of());
      channel.queueDeclare(DEAD_QUEUE, true, false, false, Map.of());
      channel.queueBind(DEAD_QUEUE, DEAD_EXCHANGE, "failed.#");
      var deadLetterArguments =
          Map.<String, Object>of(
              "x-dead-letter-exchange", DEAD_EXCHANGE,
              "x-dead-letter-routing-key", "failed.jobs");
      channel.queueDeclare("jobs", true, false, false, deadLetterArguments);
      channel.queueDeclare(DEAD_SOURCE, true, false, false, deadLetterArguments);
      channel.queueDeclare("leases", true, false, false, Map.of());
      return;
    }
    channel.exchangeDeclarePassive(DEAD_EXCHANGE);
    for (var queue : List.of("jobs", "leases", DEAD_SOURCE, DEAD_QUEUE)) {
      channel.queueDeclarePassive(queue);
    }
  }

  private static void seedAmqp(Channel channel) throws Exception {
    publish(channel, "jobs");
    var acknowledged = channel.basicGet("jobs", false);
    require(acknowledged != null, "initial ack delivery");
    channel.basicAck(acknowledged.getEnvelope().getDeliveryTag(), false);
    require(channel.basicGet("jobs", false) == null, "ack is committed before next request");
    publish(channel, "jobs");
    publish(channel, "leases");
    var leased = channel.basicGet("leases", false);
    require(leased != null && !leased.getEnvelope().isRedeliver(), "first unacknowledged lease");
    // Closing this connection leaves an unacknowledged native lease, not an implicit ack.
  }

  private static GetResponse requireMessage(Channel channel, String queue) throws Exception {
    var message = channel.basicGet(queue, false);
    require(message != null, "confirmed message survives: " + queue);
    require(Arrays.equals(message.getBody(), BODY), "AMQP binary body");
    require("application/octet-stream".equals(message.getProps().getContentType()), "content type");
    require("recovery-42".equals(message.getProps().getCorrelationId()), "correlation ID");
    require("reply-queue".equals(message.getProps().getReplyTo()), "reply-to");
    return message;
  }

  private static void verifyAmqp(Channel channel) throws Exception {
    var message = requireMessage(channel, "jobs");
    channel.basicNack(message.getEnvelope().getDeliveryTag(), false, true);
    var requeued = requireMessage(channel, "jobs");
    require(requeued.getEnvelope().isRedeliver(), "nack requeue redelivery");
    channel.basicReject(requeued.getEnvelope().getDeliveryTag(), true);
    // A synchronous RPC waits until the preceding no-response settlement was processed.
    channel.queueDeclarePassive("jobs");
    verifyNamedDeadLetter(channel);
  }

  private static void verifyNamedDeadLetter(Channel channel) throws Exception {
    publish(channel, DEAD_SOURCE);
    var source = requireMessage(channel, DEAD_SOURCE);
    channel.basicReject(source.getEnvelope().getDeliveryTag(), false);
    channel.queueDeclarePassive(DEAD_SOURCE);
    GetResponse deadLetter = null;
    var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (deadLetter == null && System.nanoTime() < deadline) {
      deadLetter = channel.basicGet(DEAD_QUEUE, false);
      if (deadLetter == null) {
        Thread.sleep(100);
      }
    }
    require(deadLetter != null, "named dead-letter delivery");
    require(
        DEAD_EXCHANGE.equals(deadLetter.getEnvelope().getExchange())
            && "failed.jobs".equals(deadLetter.getEnvelope().getRoutingKey()),
        "named dead-letter route survives gateway state");
    require(
        deadLetter.getProps().getHeaders().get("x-death") instanceof List<?> deaths
            && !deaths.isEmpty(),
        "RabbitMQ x-death history");
    require(deadLetter.getProps().getExpiration() == null, "dead-letter expiration removed");
    channel.basicAck(deadLetter.getEnvelope().getDeliveryTag(), false);
  }

  private static void settleAmqp(Channel channel) throws Exception {
    var message = requireMessage(channel, "jobs");
    channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
    require(channel.basicGet("jobs", false) == null, "settled job absent");
    GetResponse recovered = null;
    var deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
    while (recovered == null && System.nanoTime() < deadline) {
      recovered = channel.basicGet("leases", false);
      if (recovered == null) {
        Thread.sleep(100);
      }
    }
    require(recovered != null && recovered.getEnvelope().isRedeliver(), "expired lease redelivered");
    require(Arrays.equals(recovered.getBody(), BODY), "unacknowledged body preserved");
    channel.basicAck(recovered.getEnvelope().getDeliveryTag(), false);
    require(channel.basicGet("leases", false) == null, "settled lease absent");
  }

  private static void require(boolean condition, String evidence) {
    if (!condition) {
      throw new AssertionError(evidence + " did not match");
    }
  }

  private static void verifyCapacityRejection(String host, int port) throws Exception {
    var factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername("epoch");
    factory.setPassword("compat-secret");
    factory.setAutomaticRecoveryEnabled(false);
    factory.setConnectionTimeout(5_000);
    factory.setHandshakeTimeout(5_000);
    var connection = factory.newConnection("epoch-capacity-rejection");
    try {
      var channel = connection.createChannel();
      channel.queueDeclare("limited", true, false, false, Map.of());
      channel.confirmSelect();
      publish(channel, "limited");
      var rejected = false;
      try {
        channel.basicPublish("", "limited", null, BODY);
        channel.waitForConfirmsOrDie(5_000);
      } catch (java.io.IOException | ShutdownSignalException expected) {
        rejected = true;
      }
      require(rejected, "full native Queue must not confirm an unapplied message");
    } finally {
      if (connection.isOpen()) {
        connection.close();
      }
    }
    try (var verification = factory.newConnection("epoch-capacity-verification");
        var channel = verification.createChannel()) {
      var accepted = requireMessage(channel, "limited");
      channel.basicAck(accepted.getEnvelope().getDeliveryTag(), false);
      require(channel.basicGet("limited", false) == null, "rejected publish did not enter Queue");
    }
  }
}
