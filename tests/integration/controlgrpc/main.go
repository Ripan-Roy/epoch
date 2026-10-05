// Command controlgrpc drives generated management clients against real Go
// processes. The Python fault driver owns processes and durable evidence.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const proofSchema = "epoch.control-ha.grpc-api/v1"

type operationWitness struct {
	Kind      string     `json:"kind"`
	Request   []byte     `json:"request_proto"`
	Code      codes.Code `json:"grpc_code"`
	Operation []byte     `json:"operation_proto"`
}

type apiProof struct {
	Schema          string             `json:"schema"`
	ControllerCount int                `json:"controller_count"`
	Checks          map[string]bool    `json:"checks"`
	Operations      []operationWitness `json:"operations"`
	Desired         [][]byte           `json:"desired_proto"`
	WatchCheckpoint uint64             `json:"watch_checkpoint"`
	WatchCursors    []uint64           `json:"watch_matching_cursors"`
}

type clients struct {
	connections []*grpc.ClientConn
	admin       string
	reader      string
	prefix      string
	proof       apiProof
}

func main() {
	phase := flag.String("phase", "", "prepare, verify, or stale")
	endpoints := flag.String("endpoints", "", "comma-separated loopback gRPC endpoints")
	prefix := flag.String("prefix", "", "unique request-token prefix")
	count := flag.Int("controllers", 0, "original concurrent controller count")
	state := flag.String("state", "", "saved prepare proof for recovery verification")
	flag.Parse()
	if err := run(*phase, *endpoints, *prefix, *count, *state); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(phase, endpoints, prefix string, count int, state string) (runErr error) {
	if count != 3 && count != 5 || prefix == "" || endpoints == "" {
		return errors.New("requires original three/five controllers, endpoints, and token prefix")
	}
	fleet := clients{admin: os.Getenv("EPOCH_CONTROL_HA_GRPC_ADMIN_TOKEN"), reader: os.Getenv("EPOCH_CONTROL_HA_GRPC_READER_TOKEN"), prefix: prefix}
	// A failed live phase is not certification, but retaining its completed
	// observations makes the failure independently diagnosable.
	defer func() {
		if fleet.proof.Schema != "" {
			if err := json.NewEncoder(os.Stdout).Encode(fleet.proof); runErr == nil && err != nil {
				runErr = err
			}
		}
	}()
	if fleet.admin == "" || fleet.reader == "" {
		return errors.New("explicit administrator and scoped reader credentials are required")
	}
	defer fleet.close()
	for _, endpoint := range strings.Split(endpoints, ",") {
		if !strings.HasPrefix(endpoint, "127.0.0.1:") {
			return errors.New("live test clients require explicit loopback endpoints")
		}
		connection, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(5<<20)))
		if err != nil {
			return err
		}
		fleet.connections = append(fleet.connections, connection)
	}
	if phase == "prepare" {
		if len(fleet.connections) != count {
			return errors.New("prepare requires every concurrent controller")
		}
		fleet.proof = apiProof{Schema: proofSchema, ControllerCount: count, Checks: map[string]bool{}}
		if err := fleet.prepare(); err != nil {
			return err
		}
	} else {
		data, err := os.ReadFile(state)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &fleet.proof); err != nil {
			return err
		}
		if fleet.proof.Schema != proofSchema || fleet.proof.ControllerCount != count || len(fleet.proof.Operations) == 0 || len(fleet.proof.Desired) < 128 {
			return errors.New("missing or incompatible generated-client prepare evidence")
		}
		switch phase {
		case "verify":
			if err := fleet.verify(); err != nil {
				return err
			}
		case "stale":
			if err := fleet.watchFailure(0, codes.Aborted); err != nil {
				return err
			}
			fleet.proof.Checks["stale_watch_cursor_fails_closed"] = true
		default:
			return errors.New("unknown generated-client phase")
		}
	}
	return nil
}

func (fleet *clients) close() {
	for _, connection := range fleet.connections {
		_ = connection.Close()
	}
}

func (fleet *clients) client(index int) epochv1.RegionalAdminServiceClient {
	return epochv1.NewRegionalAdminServiceClient(fleet.connections[index])
}

func callContext(token string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	return ctx, cancel
}

func marshal(message proto.Message) []byte {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		panic(err)
	} // All messages are constructed or decoded by the generated contract.
	return data
}

func scopedName(name string) *epochv1.ResourceName {
	return &epochv1.ResourceName{Organization: "acme", Project: "payments", Environment: "production", Namespace: "orders", Kind: epochv1.ResourceKind_RESOURCE_KIND_CACHE, Name: name}
}

func cacheSpec(revision string) *epochv1.ResourceSpec {
	configuration, err := structpb.NewStruct(map[string]any{"shard_count": 1})
	if err != nil {
		panic(err)
	}
	return &epochv1.ResourceSpec{
		WorkloadProfile: epochv1.WorkloadProfile_WORKLOAD_PROFILE_CACHE,
		Durability:      epochv1.DurabilityProfile_DURABILITY_PROFILE_QUORUM_DURABLE,
		Replicas:        3, Configuration: configuration, Labels: map[string]string{"ha_revision": revision},
		Governance: &epochv1.ResourceGovernance{Owner: "team:ha-certification", CostCenter: "cc-ha", Classification: epochv1.DataClassification_DATA_CLASSIFICATION_INTERNAL},
		Placement:  &epochv1.PlacementPolicy{AllowedRegions: []string{"ap-south"}, MinimumZones: 3, RequiredNodeClass: "general-purpose"},
	}
}

func batchItem(name *epochv1.ResourceName, revision string, generation uint64) *epochv1.BatchApplyResource {
	return &epochv1.BatchApplyResource{Name: name, Spec: cacheSpec(revision), ExpectedGeneration: &generation}
}

// Start gates issue a workload from every endpoint without assuming which
// transport or Catalog proposal wins. Result order remains deterministic.
func concurrently[T any](count int, call func(int) (T, error)) ([]T, []error) {
	results, failures := make([]T, count), make([]error, count)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(count)
	done.Add(count)
	for index := range count {
		go func() { defer done.Done(); ready.Done(); <-start; results[index], failures[index] = call(index) }()
	}
	ready.Wait()
	close(start)
	done.Wait()
	return results, failures
}

func (fleet *clients) apply(index int, request *epochv1.BatchApplyResourcesRequest) (*epochv1.BatchApplyResourcesResponse, error) {
	ctx, cancel := callContext(fleet.admin)
	defer cancel()
	return fleet.client(index).BatchApplyResources(ctx, request)
}

func (fleet *clients) prepare() error {
	initial := &epochv1.BatchApplyResourcesRequest{RequestToken: fleet.prefix + "-maximum"}
	for index := range 128 {
		initial.Resources = append(initial.Resources, batchItem(scopedName(fmt.Sprintf("ha-grpc-item-%03d", index)), "initial", 0))
	}
	results, failures := concurrently(len(fleet.connections), func(index int) (*epochv1.BatchApplyResourcesResponse, error) { return fleet.apply(index, initial) })
	newOutcomes := 0
	for index, response := range results {
		if failures[index] != nil {
			return fmt.Errorf("maximum batch at controller %d: %w", index, failures[index])
		}
		if len(response.GetResults()) != 128 {
			return errors.New("maximum batch returned a partial result")
		}
		if !response.GetReplayed() {
			newOutcomes++
		}
		for _, result := range response.GetResults() {
			if result.GetResource().GetGeneration() != 1 || !result.GetCreated() || !result.GetChanged() {
				return errors.New("maximum batch altered its retained creation outcome")
			}
		}
	}
	if newOutcomes != 1 {
		return fmt.Errorf("identical concurrent tokens created %d independent outcomes", newOutcomes)
	}
	if err := fleet.captureBatch(initial, codes.OK); err != nil {
		return err
	}
	conflicting := proto.Clone(initial).(*epochv1.BatchApplyResourcesRequest)
	conflicting.Resources[0].Spec.Labels["ha_revision"] = "conflicting-bytes"
	if _, err := fleet.apply(0, conflicting); status.Code(err) != codes.Aborted {
		return fmt.Errorf("conflicting token code = %s", status.Code(err))
	}
	fleet.proof.Checks["concurrent_maximum_batch_exact_replay"] = true

	contenders := make([]*epochv1.BatchApplyResourcesRequest, len(fleet.connections))
	for index := range contenders {
		contenders[index] = &epochv1.BatchApplyResourcesRequest{RequestToken: fmt.Sprintf("%s-occ-%d", fleet.prefix, index), Resources: []*epochv1.BatchApplyResource{
			batchItem(initial.Resources[0].Name, fmt.Sprintf("winner-%d", index), 1),
			batchItem(initial.Resources[index+1].Name, fmt.Sprintf("winner-%d", index), 1),
		}}
	}
	_, failures = concurrently(len(contenders), func(index int) (*epochv1.BatchApplyResourcesResponse, error) {
		return fleet.apply(index, contenders[index])
	})
	winner := -1
	for index, err := range failures {
		code := status.Code(err)
		if code == codes.OK {
			if winner != -1 {
				return errors.New("two conflicting OCC batches succeeded")
			}
			winner = index
		} else if code != codes.Aborted {
			return fmt.Errorf("OCC controller %d code = %s", index, code)
		}
		if err := fleet.captureBatch(contenders[index], code); err != nil {
			return err
		}
	}
	if winner == -1 {
		return errors.New("no OCC batch succeeded")
	}
	for index, item := range initial.Resources {
		generation, revision := uint64(1), "initial"
		if index == 0 || index == winner+1 {
			generation, revision = 2, fmt.Sprintf("winner-%d", winner)
		}
		if err := fleet.captureDesired(item.Name, generation, cacheSpec(revision)); err != nil {
			return err
		}
	}
	fleet.proof.Checks["concurrent_occ_batches_atomic"] = true
	if err := fleet.authorizeOperations(); err != nil {
		return err
	}
	if err := fleet.deleteRecreate(); err != nil {
		return err
	}
	if err := fleet.watchResume(initial.Resources[120].Name, initial.Resources[121].Name); err != nil {
		return err
	}
	if err := fleet.watchFailure(^uint64(0), codes.InvalidArgument); err != nil {
		return err
	}
	fleet.proof.Checks["future_watch_cursor_fails_closed"] = true
	return fleet.verify()
}

func (fleet *clients) captureBatch(request *epochv1.BatchApplyResourcesRequest, code codes.Code) error {
	names := make([]*epochv1.ResourceName, 0, len(request.Resources))
	for _, item := range request.Resources {
		names = append(names, item.Name)
	}
	operation, err := fleet.operation(0, fleet.admin, request.RequestToken, names)
	if err != nil {
		return err
	}
	state := epochv1.OperationState_OPERATION_STATE_SUCCEEDED
	if code != codes.OK {
		state = epochv1.OperationState_OPERATION_STATE_FAILED
	}
	if operation.GetState() != state || operation.GetCommandKind() != "apply_desired" || operation.ExpectedGeneration != nil || code != codes.OK && operation.GetFailureCode() == "" {
		return errors.New("batch operation lacks its exact durable command/outcome identity")
	}
	fleet.proof.Operations = append(fleet.proof.Operations, operationWitness{Kind: "batch", Request: marshal(request), Code: code, Operation: marshal(operation)})
	return nil
}

func (fleet *clients) operation(index int, token, requestToken string, names []*epochv1.ResourceName) (*epochv1.GetOperationResponse, error) {
	ctx, cancel := callContext(token)
	defer cancel()
	return fleet.client(index).GetOperation(ctx, &epochv1.GetOperationRequest{RequestToken: requestToken, AffectedResources: names})
}

func (fleet *clients) get(index int, name *epochv1.ResourceName) (*epochv1.Resource, error) {
	ctx, cancel := callContext(fleet.admin)
	defer cancel()
	response, err := fleet.client(index).GetResource(ctx, &epochv1.GetResourceRequest{Name: name})
	if err != nil {
		return nil, err
	}
	return desiredWitness(response.GetResource())
}

func (fleet *clients) captureDesired(name *epochv1.ResourceName, generation uint64, spec *epochv1.ResourceSpec) error {
	expected := &epochv1.Resource{Name: name, Generation: generation, Spec: spec}
	for index := range fleet.connections {
		observed, err := fleet.get(index, name)
		if err != nil || !proto.Equal(observed, expected) {
			return fmt.Errorf("desired state differs at controller %d for %s: %v", index, name.GetName(), err)
		}
	}
	// Later updates replace this witness rather than comparing stale desired
	// generations. Historical outcomes remain separately retained above.
	for index, data := range fleet.proof.Desired {
		old := &epochv1.Resource{}
		if err := proto.Unmarshal(data, old); err != nil {
			return err
		}
		if proto.Equal(old.Name, name) {
			fleet.proof.Desired[index] = marshal(expected)
			return nil
		}
	}
	fleet.proof.Desired = append(fleet.proof.Desired, marshal(expected))
	return nil
}

func (fleet *clients) authorizeOperations() error {
	operation := &epochv1.GetOperationResponse{}
	if err := proto.Unmarshal(fleet.proof.Operations[0].Operation, operation); err != nil {
		return err
	}
	for index := range fleet.connections {
		got, err := fleet.operation(index, fleet.reader, operation.RequestToken, operation.AffectedResources)
		if err != nil {
			return err
		}
		if err := compareOperation(operation, got); err != nil {
			return err
		}
		for _, test := range []struct {
			token string
			names []*epochv1.ResourceName
			code  codes.Code
		}{
			{fleet.admin, operation.AffectedResources[:1], codes.NotFound},
			{fleet.admin, nil, codes.InvalidArgument},
			{fleet.admin, []*epochv1.ResourceName{operation.AffectedResources[0], operation.AffectedResources[0]}, codes.InvalidArgument},
			{"", operation.AffectedResources, codes.Unauthenticated},
		} {
			_, err := fleet.operation(index, test.token, operation.RequestToken, test.names)
			if status.Code(err) != test.code {
				return fmt.Errorf("operation authorization at controller %d code = %s, want %s", index, status.Code(err), test.code)
			}
		}
	}
	allowed, denied := scopedName("ha-grpc-authorization"), scopedName("ha-grpc-authorization")
	denied.Organization = "otherco"
	request := &epochv1.BatchApplyResourcesRequest{RequestToken: fleet.prefix + "-mixed-scope", Resources: []*epochv1.BatchApplyResource{batchItem(allowed, "scope", 0), batchItem(denied, "scope", 0)}}
	if _, err := fleet.apply(0, request); err != nil {
		return err
	}
	if err := fleet.captureBatch(request, codes.OK); err != nil {
		return err
	}
	for index := range fleet.connections {
		if _, err := fleet.operation(index, fleet.reader, request.RequestToken, []*epochv1.ResourceName{allowed, denied}); status.Code(err) != codes.PermissionDenied {
			return errors.New("mixed-tenant operation was disclosed to scoped reader")
		}
		if _, err := fleet.operation(index, fleet.reader, request.RequestToken, []*epochv1.ResourceName{allowed}); status.Code(err) != codes.NotFound {
			return errors.New("partial affected-resource set disclosed mixed-tenant operation")
		}
	}
	for _, name := range []*epochv1.ResourceName{allowed, denied} {
		if err := fleet.captureDesired(name, 1, cacheSpec("scope")); err != nil {
			return err
		}
	}
	fleet.proof.Checks["operation_exact_resource_authorization"] = true
	return nil
}

func (fleet *clients) delete(index int, request *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
	ctx, cancel := callContext(fleet.admin)
	defer cancel()
	return fleet.client(index).DeleteResource(ctx, request)
}

func (fleet *clients) captureDelete(request *epochv1.DeleteResourceRequest, kind string) error {
	operation, err := fleet.operation(0, fleet.admin, request.RequestToken, []*epochv1.ResourceName{request.Name})
	if err != nil {
		return err
	}
	if operation.GetState() != epochv1.OperationState_OPERATION_STATE_SUCCEEDED || operation.GetCommandKind() != kind ||
		(operation.ExpectedGeneration == nil) != (request.ExpectedGeneration == nil) || request.ExpectedGeneration != nil && operation.GetExpectedGeneration() != *request.ExpectedGeneration {
		return errors.New("delete operation altered command kind or original optional precondition")
	}
	fleet.proof.Operations = append(fleet.proof.Operations, operationWitness{Kind: "delete", Request: marshal(request), Code: codes.OK, Operation: marshal(operation)})
	return nil
}

func (fleet *clients) createOne(name *epochv1.ResourceName, suffix string, generation uint64) error {
	request := &epochv1.BatchApplyResourcesRequest{RequestToken: fleet.prefix + "-" + suffix, Resources: []*epochv1.BatchApplyResource{batchItem(name, suffix, 0)}}
	response, err := fleet.apply(0, request)
	if err != nil {
		return err
	}
	if len(response.GetResults()) != 1 || response.Results[0].GetResource().GetGeneration() != generation {
		return errors.New("recreated generation was reused")
	}
	if err := fleet.captureBatch(request, codes.OK); err != nil {
		return err
	}
	return fleet.captureDesired(name, generation, cacheSpec(suffix))
}

func (fleet *clients) deleteRecreate() error {
	zero, one := uint64(0), uint64(1)
	for _, test := range []struct {
		suffix       string
		missing      bool
		precondition *uint64
	}{
		{"missing-omitted", true, nil}, {"missing-zero", true, &zero}, {"completed-omitted", false, nil}, {"completed-one", false, &one},
	} {
		name := scopedName("ha-grpc-" + test.suffix)
		if !test.missing {
			if err := fleet.createOne(name, test.suffix+"-create", 1); err != nil {
				return err
			}
		}
		request := &epochv1.DeleteResourceRequest{RequestToken: fleet.prefix + "-" + test.suffix + "-delete", Name: name, ExpectedGeneration: test.precondition}
		deleted, err := fleet.delete(0, request)
		if err != nil {
			return err
		}
		if deleted.GetDeleted() == test.missing {
			return errors.New("delete did not preserve missing/live distinction")
		}
		kind, generation := "delete_desired", uint64(1)
		if !test.missing {
			kind, generation = "delete_managed", 3
		}
		if err := fleet.captureDelete(request, kind); err != nil {
			return err
		}
		if err := fleet.createOne(name, test.suffix+"-recreate", generation); err != nil {
			return err
		}
		results, failures := concurrently(len(fleet.connections), func(index int) (*epochv1.DeleteResourceResponse, error) { return fleet.delete(index, request) })
		for index, replay := range results {
			if failures[index] != nil || !replay.GetReplayed() || replay.GetDeleted() != deleted.GetDeleted() || replay.GetGeneration() != deleted.GetGeneration() {
				return fmt.Errorf("old delete changed outcome at controller %d: %v", index, failures[index])
			}
		}
		conflicting := proto.Clone(request).(*epochv1.DeleteResourceRequest)
		if request.ExpectedGeneration == nil {
			conflicting.ExpectedGeneration = &zero
		} else {
			conflicting.ExpectedGeneration = nil
		}
		if _, err := fleet.delete(0, conflicting); status.Code(err) != codes.Aborted {
			return fmt.Errorf("changed optional precondition code = %s", status.Code(err))
		}
		if err := fleet.captureDesired(name, generation, cacheSpec(test.suffix+"-recreate")); err != nil {
			return err
		}
	}
	fleet.proof.Checks["delete_recreate_old_tokens_safe"] = true
	fleet.proof.Checks["delete_operation_precondition_presence"] = true
	return nil
}

func (fleet *clients) watch(index int, after, target uint64) (uint64, []uint64, bool, error) {
	ctx, cancel := callContext(fleet.reader)
	defer cancel()
	stream, err := fleet.client(index).WatchResourceChanges(ctx, &epochv1.WatchResourceChangesRequest{AfterCursor: after, BatchSize: 2})
	if err != nil {
		return 0, nil, false, err
	}
	cursor, seen, scannedFiltered := after, []uint64{}, false
	for {
		page, err := stream.Recv()
		if err != nil {
			return 0, nil, false, err
		}
		next, err := observeWatch(cursor, page, scopedName(""))
		if err != nil {
			return 0, nil, false, err
		}
		if target == 0 {
			target = page.GetLatestCursor()
		}
		for _, change := range page.GetChanges() {
			seen = append(seen, change.GetCursor())
		}
		if watchPageFiltered(cursor, page) {
			scannedFiltered = true
		}
		cursor = next
		if cursor >= target {
			return cursor, seen, scannedFiltered, nil
		}
	}
}

func (fleet *clients) watchResume(first, second *epochv1.ResourceName) error {
	cursor, _, _, err := fleet.watch(0, 0, 0)
	if err != nil {
		return err
	}
	hidden := scopedName("ha-grpc-watch-hidden")
	hidden.Organization = "otherco"
	request := &epochv1.BatchApplyResourcesRequest{RequestToken: fleet.prefix + "-watch-first", Resources: []*epochv1.BatchApplyResource{batchItem(first, "watch-first", 1), batchItem(hidden, "watch-hidden", 0)}}
	if _, err := fleet.apply(0, request); err != nil {
		return err
	}
	if err := fleet.captureBatch(request, codes.OK); err != nil {
		return err
	}
	operation, err := fleet.operation(0, fleet.admin, request.RequestToken, []*epochv1.ResourceName{first, hidden})
	if err != nil {
		return err
	}
	if operation.GetLastChangeCursor() != operation.GetFirstChangeCursor()+1 {
		return errors.New("mixed watch mutation lacks two contiguous committed changes")
	}
	cursor, seen, filtered, err := fleet.watch(len(fleet.connections)-1, cursor, operation.GetLastChangeCursor())
	if err != nil {
		return err
	}
	if !slices.Contains(seen, operation.GetFirstChangeCursor()) || slices.Contains(seen, operation.GetLastChangeCursor()) || !filtered {
		return errors.New("watch skipped visible change, exposed other tenant, or lost scanned cursor")
	}
	wanted := []uint64{operation.GetFirstChangeCursor()}
	// Disconnect again, mutate through another controller, then resume at the
	// scanned checkpoint rather than either latest or last visible cursor.
	request = &epochv1.BatchApplyResourcesRequest{RequestToken: fleet.prefix + "-watch-second", Resources: []*epochv1.BatchApplyResource{batchItem(second, "watch-second", 1)}}
	if _, err := fleet.apply(1, request); err != nil {
		return err
	}
	if err := fleet.captureBatch(request, codes.OK); err != nil {
		return err
	}
	operation, err = fleet.operation(0, fleet.admin, request.RequestToken, []*epochv1.ResourceName{second})
	if err != nil {
		return err
	}
	resumed, changes, _, err := fleet.watch(1, cursor, operation.GetLastChangeCursor())
	if err != nil {
		return err
	}
	if !slices.Contains(changes, operation.GetFirstChangeCursor()) || slices.Contains(changes, wanted[0]) {
		return errors.New("watch reconnect skipped or duplicated an acknowledged matching change")
	}
	wanted = append(wanted, operation.GetFirstChangeCursor())
	fleet.proof.WatchCheckpoint, fleet.proof.WatchCursors = resumed, wanted
	for _, item := range request.Resources {
		if err := fleet.captureDesired(item.Name, 2, item.Spec); err != nil {
			return err
		}
	}
	if err := fleet.captureDesired(first, 2, cacheSpec("watch-first")); err != nil {
		return err
	}
	if err := fleet.captureDesired(hidden, 1, cacheSpec("watch-hidden")); err != nil {
		return err
	}
	fleet.proof.Checks["authorized_watch_disconnect_resume"] = true
	return nil
}

func (fleet *clients) watchFailure(after uint64, code codes.Code) error {
	for index := range fleet.connections {
		ctx, cancel := callContext(fleet.reader)
		stream, err := fleet.client(index).WatchResourceChanges(ctx, &epochv1.WatchResourceChangesRequest{AfterCursor: after, BatchSize: 2})
		if err == nil {
			_, err = stream.Recv()
		}
		cancel()
		if status.Code(err) != code {
			return fmt.Errorf("invalid watch checkpoint at controller %d code = %s, want %s", index, status.Code(err), code)
		}
	}
	return nil
}

func (fleet *clients) verify() error {
	for index := range fleet.connections {
		for _, witness := range fleet.proof.Operations {
			expected := &epochv1.GetOperationResponse{}
			if err := proto.Unmarshal(witness.Operation, expected); err != nil {
				return err
			}
			observed, err := fleet.operation(index, fleet.admin, expected.RequestToken, expected.AffectedResources)
			if err != nil {
				return err
			}
			if err := compareOperation(expected, observed); err != nil {
				return err
			}
			switch witness.Kind {
			case "batch":
				request := &epochv1.BatchApplyResourcesRequest{}
				if err := proto.Unmarshal(witness.Request, request); err != nil {
					return err
				}
				result, err := fleet.apply(index, request)
				if status.Code(err) != witness.Code || err == nil && !result.GetReplayed() {
					return fmt.Errorf("batch retained outcome changed at controller %d", index)
				}
			case "delete":
				request := &epochv1.DeleteResourceRequest{}
				if err := proto.Unmarshal(witness.Request, request); err != nil {
					return err
				}
				result, err := fleet.delete(index, request)
				if err != nil || !result.GetReplayed() {
					return fmt.Errorf("delete retained outcome changed at controller %d: %v", index, err)
				}
			default:
				return errors.New("unknown retained request kind")
			}
		}
		for _, data := range fleet.proof.Desired {
			expected := &epochv1.Resource{}
			if err := proto.Unmarshal(data, expected); err != nil {
				return err
			}
			observed, err := fleet.get(index, expected.Name)
			if err != nil || !proto.Equal(expected, observed) {
				return fmt.Errorf("desired state lost or partially changed at controller %d for %s: %v", index, expected.GetName().GetName(), err)
			}
		}
	}
	fleet.proof.Checks["durable_operations_and_desired_verified"] = true
	return nil
}

func compareOperation(expected, actual *epochv1.GetOperationResponse) error {
	if expected == nil || actual == nil || !proto.Equal(expected, actual) {
		return errors.New("durable operation identity/outcome differs from its original receipt")
	}
	return nil
}

func desiredWitness(resource *epochv1.Resource) (*epochv1.Resource, error) {
	if resource == nil || resource.GetName() == nil || resource.GetSpec() == nil || resource.GetGeneration() == 0 {
		return nil, errors.New("incomplete desired-state witness")
	}
	copy := proto.Clone(resource).(*epochv1.Resource)
	copy.Status = nil
	return copy, nil
}

func observeWatch(after uint64, page *epochv1.WatchResourceChangesResponse, scope *epochv1.ResourceName) (uint64, error) {
	if page == nil || scope == nil || page.GetNextCursor() < after || page.GetNextCursor() > page.GetLatestCursor() {
		return 0, errors.New("invalid scanned watch checkpoint")
	}
	previous := after
	for _, change := range page.GetChanges() {
		name := change.GetName()
		if name == nil || change.GetCursor() <= previous || change.GetCursor() > page.GetNextCursor() || name.GetOrganization() != scope.GetOrganization() || name.GetProject() != scope.GetProject() || name.GetEnvironment() != scope.GetEnvironment() || name.GetNamespace() != scope.GetNamespace() {
			return 0, errors.New("unordered, skipped-checkpoint, or cross-tenant watch change")
		}
		previous = change.GetCursor()
	}
	return page.GetNextCursor(), nil
}

func watchPageFiltered(after uint64, page *epochv1.WatchResourceChangesResponse) bool {
	// Global changes are contiguous before authorization filtering. A hidden
	// change can be in the middle, not only after the last visible change.
	return page.GetNextCursor() >= after && page.GetNextCursor()-after > uint64(len(page.GetChanges()))
}
