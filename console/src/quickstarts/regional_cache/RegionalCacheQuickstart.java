import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.epoch.sdk.RegionalCacheClient;
import io.epoch.sdk.RegionalCacheExpectation;
import io.epoch.sdk.RegionalCacheLockGuard;
import io.epoch.sdk.RegionalCacheMutation;
import io.epoch.sdk.RegionalCacheValue;
import io.epoch.sdk.RegionalScope;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class RegionalCacheQuickstart {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    List<URI> endpoints =
        Arrays.stream(
                environment(
                        "EPOCH_REGIONAL_ENDPOINTS",
                        "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663")
                    .split(","))
            .map(String::trim)
            .map(URI::create)
            .toList();
    RegionalCacheClient client =
        new RegionalCacheClient(
            endpoints,
            environment("EPOCH_TOKEN", "epoch-dev-admin-v1"),
            new RegionalScope("acme", "shop", "dev", "core"),
            Duration.ofSeconds(3));

    JsonNode written =
        client.set(
            "sessions",
            0,
            "docs-java-cache-set-v1",
            "profile",
            RegionalCacheValue.string("alice"),
            null,
            null);
    JsonNode replayed =
        client.set(
            "sessions",
            0,
            "docs-java-cache-set-v1",
            "profile",
            RegionalCacheValue.string("alice"),
            null,
            null);
    BigInteger version = new BigInteger(result(written).path("item").path("version").asText());
    JsonNode compared =
        client.compareAndSet(
            "sessions",
            0,
            "docs-java-cache-cas-v1",
            "profile",
            RegionalCacheExpectation.version(version),
            RegionalCacheValue.hash(Map.of("name", "alice", "role", "admin")),
            null,
            null);
    JsonNode observation = client.observe("sessions", 0, "profile");
    BigInteger revision =
        new BigInteger(observation.path("observation").path("shard_revision").asText());
    JsonNode transaction =
        client.transaction(
            "sessions",
            0,
            "docs-java-cache-transaction-v1",
            revision,
            List.of(
                RegionalCacheMutation.set("visits", RegionalCacheValue.counter(1), null),
                RegionalCacheMutation.set(
                    "recent", RegionalCacheValue.list(List.of("home", "checkout")), null),
                RegionalCacheMutation.set(
                    "roles", RegionalCacheValue.set(List.of("admin", "buyer")), null),
                RegionalCacheMutation.set(
                    "rank", RegionalCacheValue.sortedSet(Map.of("alice", 9.5)), null),
                RegionalCacheMutation.set(
                    "avatar", RegionalCacheValue.blob("epoch".getBytes()), null)),
            List.of());
    JsonNode acquired =
        client.acquireLock(
            "sessions",
            0,
            "docs-java-cache-lock-v1",
            "profile-lock",
            "docs-java",
            BigInteger.ONE,
            BigInteger.valueOf(60_000));
    String leaseToken = result(acquired).path("lease_token").asText();
    RegionalCacheLockGuard guard =
        new RegionalCacheLockGuard("profile-lock", "docs-java", BigInteger.ONE, leaseToken);
    JsonNode guarded =
        client.increment(
            "sessions",
            0,
            "docs-java-cache-guarded-increment-v1",
            "visits",
            1,
            null,
            null,
            guard);
    JsonNode released =
        client.releaseLock(
            "sessions",
            0,
            "docs-java-cache-release-v1",
            "profile-lock",
            "docs-java",
            BigInteger.ONE,
            leaseToken);
    JsonNode ephemeral =
        client.set(
            "sessions",
            0,
            "docs-java-cache-ttl-v1",
            "flash",
            RegionalCacheValue.string("short"),
            BigInteger.ONE,
            null);
    Thread.sleep(10);
    JsonNode maintained = client.maintain("sessions", 0, "docs-java-cache-maintain-v1", 100);

    ObjectNode output = MAPPER.createObjectNode();
    output.set("set", written);
    output.set("exact_retry", replayed);
    output.set("cas", compared);
    output.set("transaction", transaction);
    output.set("guarded_increment", guarded);
    output.set("release", released);
    output.set("ttl", ephemeral);
    output.set("maintain", maintained);
    output.set("profile", client.observe("sessions", 0, "profile"));
    output.set("status", client.status("sessions", 0));
    System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(output));
  }

  private static JsonNode result(JsonNode document) {
    return document.path("receipt").path("outcome").path("result");
  }

  private static String environment(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }
}
