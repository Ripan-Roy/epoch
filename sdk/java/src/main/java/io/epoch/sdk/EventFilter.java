package io.epoch.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;

/** Typed Event Bus subscription filter. */
public record EventFilter(
    List<String> eventTypePatterns,
    List<String> sourcePatterns,
    List<String> subjectPatterns,
    Map<String, String> headers,
    Map<String, Object> jsonEquals) {

  public EventFilter {
    eventTypePatterns = List.copyOf(eventTypePatterns);
    sourcePatterns = List.copyOf(sourcePatterns);
    subjectPatterns = List.copyOf(subjectPatterns);
    headers = Map.copyOf(headers);
    jsonEquals = Map.copyOf(jsonEquals);
  }

  public static EventFilter empty() {
    return new EventFilter(List.of(), List.of(), List.of(), Map.of(), Map.of());
  }

  ObjectNode toJson(ObjectMapper mapper) {
    ObjectNode value = mapper.createObjectNode();
    value.set("event_type_patterns", mapper.valueToTree(eventTypePatterns));
    value.set("source_patterns", mapper.valueToTree(sourcePatterns));
    value.set("subject_patterns", mapper.valueToTree(subjectPatterns));
    value.set("headers", mapper.valueToTree(headers));
    value.set("json_equals", mapper.valueToTree(jsonEquals));
    return value;
  }
}
