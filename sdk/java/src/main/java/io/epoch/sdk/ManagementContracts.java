package io.epoch.sdk;

import io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse;
import io.epoch.sdk.gen.epoch.v1.DataClassification;
import io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest;
import io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse;
import io.epoch.sdk.gen.epoch.v1.GetOperationRequest;
import io.epoch.sdk.gen.epoch.v1.GetOperationResponse;
import io.epoch.sdk.gen.epoch.v1.ListResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.ListResourcesResponse;
import io.epoch.sdk.gen.epoch.v1.OperationState;
import io.epoch.sdk.gen.epoch.v1.Resource;
import io.epoch.sdk.gen.epoch.v1.ResourceChange;
import io.epoch.sdk.gen.epoch.v1.ResourceChangeKind;
import io.epoch.sdk.gen.epoch.v1.ResourceKind;
import io.epoch.sdk.gen.epoch.v1.ResourceName;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Presence-aware request/receipt contracts shared by unary calls and watches. */
final class ManagementContracts {
  private ManagementContracts() {}

  static void require(boolean valid) {
    if (!valid) {
      throw ManagementRPCException.local(Status.INVALID_ARGUMENT);
    }
  }

  static void receipt(boolean valid) {
    if (!valid) {
      throw Status.DATA_LOSS.asRuntimeException();
    }
  }

  static void token(String token) {
    require(
        !token.isEmpty()
            && token.equals(token.strip())
            && StandardCharsets.UTF_8.newEncoder().canEncode(token)
            && token.getBytes(StandardCharsets.UTF_8).length <= 256);
  }

  static boolean validName(ResourceName name) {
    if (name.getKindValue() == 0 || ResourceKind.forNumber(name.getKindValue()) == null) {
      return false;
    }
    for (String segment :
        List.of(
            name.getOrganization(),
            name.getProject(),
            name.getEnvironment(),
            name.getNamespace(),
            name.getName())) {
      if (segment.isEmpty()
          || !segment.equals(segment.strip())
          || !StandardCharsets.UTF_8.newEncoder().canEncode(segment)
          || segment.indexOf('/') >= 0
          || segment.indexOf('\0') >= 0
          || segment.indexOf('\r') >= 0
          || segment.indexOf('\n') >= 0) {
        return false;
      }
    }
    return true;
  }

  static void name(ResourceName name) {
    require(validName(name));
  }

  static boolean validNames(List<ResourceName> names) {
    return !names.isEmpty()
        && names.size() <= 128
        && names.stream().allMatch(ManagementContracts::validName)
        && new HashSet<>(names).size() == names.size();
  }

  static void names(List<ResourceName> names) {
    require(validNames(names));
  }

  static void listRequest(ListResourcesRequest request) {
    require(request != null && request.getPageSize() >= 0 && request.getPageSize() <= 100);
    require(
        ResourceKind.forNumber(request.getKindValue()) != null
            && DataClassification.forNumber(request.getClassificationValue()) != null);
  }

  static void watchRequest(WatchResourceChangesRequest request) {
    require(
        request != null
            && Integer.compareUnsigned(request.getBatchSize(), 1000) <= 0
            && ResourceKind.forNumber(request.getKindValue()) != null);
    require(request.getSerializedSize() <= ManagementClient.MAX_MESSAGE_BYTES);
  }

  static void resource(Resource resource) {
    receipt(
        resource.hasName()
            && validName(resource.getName())
            && resource.hasSpec()
            && resource.getGeneration() != 0);
  }

  static void apply(ResourceName name, ApplyResourceResponse response) {
    receipt(response.hasResource());
    resource(response.getResource());
    receipt(name.equals(response.getResource().getName()));
  }

  static boolean scope(
      ResourceName name,
      String organization,
      String project,
      String environment,
      String namespace,
      int kind) {
    return (organization.isEmpty() || organization.equals(name.getOrganization()))
        && (project.isEmpty() || project.equals(name.getProject()))
        && (environment.isEmpty() || environment.equals(name.getEnvironment()))
        && (namespace.isEmpty() || namespace.equals(name.getNamespace()))
        && (kind == 0 || kind == name.getKindValue());
  }

  static void list(ListResourcesRequest request, ListResourcesResponse response) {
    int limit = request.getPageSize() == 0 ? 50 : request.getPageSize();
    receipt(response.getResourcesCount() <= limit);
    Set<ResourceName> seen = new HashSet<>();
    for (Resource item : response.getResourcesList()) {
      resource(item);
      receipt(
          seen.add(item.getName())
              && scope(
                  item.getName(),
                  request.getOrganization(),
                  request.getProject(),
                  request.getEnvironment(),
                  request.getNamespace(),
                  request.getKindValue()));
      var governance = item.getSpec().getGovernance();
      receipt(
          (request.getOwner().isEmpty() || request.getOwner().equals(governance.getOwner()))
              && (request.getCostCenter().isEmpty()
                  || request.getCostCenter().equals(governance.getCostCenter()))
              && (request.getClassificationValue() == 0
                  || request.getClassificationValue() == governance.getClassificationValue()));
      for (var entry : request.getTagsMap().entrySet()) {
        receipt(
            governance.containsTags(entry.getKey())
                && entry.getValue().equals(governance.getTagsOrThrow(entry.getKey())));
      }
    }
  }

  static void delete(DeleteResourceRequest request, DeleteResourceResponse response) {
    receipt(
        request.getName().equals(response.getName())
            && (!response.getDeleted() || response.getGeneration() != 0));
  }

  static void batch(BatchApplyResourcesRequest request, BatchApplyResourcesResponse response) {
    receipt(response.getResultsCount() == request.getResourcesCount());
    Set<ResourceName> remaining = new HashSet<>();
    request.getResourcesList().forEach(item -> remaining.add(item.getName()));
    for (ApplyResourceResponse result : response.getResultsList()) {
      receipt(result.hasResource());
      resource(result.getResource());
      receipt(
          remaining.remove(result.getResource().getName())
              && result.getReplayed() == response.getReplayed());
    }
    receipt(remaining.isEmpty());
  }

  static void operation(GetOperationRequest request, GetOperationResponse response) {
    receipt(
        response.getRequestToken().equals(request.getRequestToken())
            && response.getProposalId() != 0
            && !response.getCommandKind().isEmpty()
            && response.getStateValue() >= OperationState.OPERATION_STATE_PENDING_VALUE
            && response.getStateValue() <= OperationState.OPERATION_STATE_FAILED_VALUE
            && validNames(response.getAffectedResourcesList())
            && new HashSet<>(response.getAffectedResourcesList())
                .equals(new HashSet<>(request.getAffectedResourcesList()))
            && Long.compareUnsigned(response.getFirstChangeCursor(), response.getLastChangeCursor())
                <= 0
            && (response.getFirstChangeCursor() != 0 || response.getLastChangeCursor() == 0));
    if (response.getState() == OperationState.OPERATION_STATE_FAILED) {
      receipt(!response.getFailureCode().isEmpty());
    } else {
      receipt(response.getFailureCode().isEmpty() && response.getFailureMessage().isEmpty());
    }
  }

  static void watch(
      WatchResourceChangesRequest filter, long after, WatchResourceChangesResponse page) {
    long earliest = page.getEarliestCursor();
    long latest = page.getLatestCursor();
    long next = page.getNextCursor();
    receipt(
        earliest != 0
            && Long.compareUnsigned(earliest - 1, after) <= 0
            && (Long.compareUnsigned(earliest, latest) <= 0 || (earliest == 1 && latest == 0))
            && Long.compareUnsigned(next, after) >= 0
            && Long.compareUnsigned(next, latest) <= 0);
    int limit = filter.getBatchSize() == 0 ? 100 : filter.getBatchSize();
    receipt(page.getChangesCount() <= limit);
    long previous = after;
    for (ResourceChange change : page.getChangesList()) {
      receipt(
          Long.compareUnsigned(change.getCursor(), previous) > 0
              && Long.compareUnsigned(change.getCursor(), next) <= 0
              && change.getGeneration() != 0
              && change.getKindValue()
                  >= ResourceChangeKind.RESOURCE_CHANGE_KIND_DESIRED_APPLIED_VALUE
              && change.getKindValue()
                  <= ResourceChangeKind.RESOURCE_CHANGE_KIND_STATUS_UPDATED_VALUE
              && validName(change.getName())
              && scope(
                  change.getName(),
                  filter.getOrganization(),
                  filter.getProject(),
                  filter.getEnvironment(),
                  filter.getNamespace(),
                  filter.getKindValue()));
      previous = change.getCursor();
    }
  }
}
