package resources

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestApplyIsIdempotentByRequestToken(t *testing.T) {
	registry := NewRegistry()
	request := ApplyRequest{
		RequestToken: "create-orders",
		Resource: desired(ResourceKey{Namespace: "prod", Kind: KindStream, Name: "orders"}, `{
			"partitions": 3,
			"retention": "30d"
		}`),
	}

	created, err := registry.Apply(request)
	if err != nil {
		t.Fatalf("Apply(create) error = %v", err)
	}
	if !created.Created || !created.Changed || created.Replayed {
		t.Fatalf("Apply(create) flags = %+v", created)
	}
	if created.Resource.Generation != 1 {
		t.Fatalf("generation = %d, want 1", created.Resource.Generation)
	}

	replayed, err := registry.Apply(request)
	if err != nil {
		t.Fatalf("Apply(replay) error = %v", err)
	}
	if !replayed.Replayed || replayed.Resource.Generation != 1 {
		t.Fatalf("Apply(replay) = %+v", replayed)
	}
	if registry.Count() != 1 {
		t.Fatalf("Count() = %d, want 1", registry.Count())
	}

	changedRequest := request
	changedRequest.Resource.Spec = json.RawMessage(`{"partitions":6}`)
	_, err = registry.Apply(changedRequest)
	assertCode(t, err, CodeConflict)
}

func TestApplyUsesExpectedGenerationAndCanonicalDesiredState(t *testing.T) {
	registry := NewRegistry()
	key := ResourceKey{Namespace: "prod", Kind: KindQueue, Name: "payments"}
	created, err := registry.Apply(ApplyRequest{
		RequestToken:       "create-payments",
		ExpectedGeneration: uint64Pointer(0),
		Resource:           desired(key, `{"max_attempts":8,"durability":"quorum"}`),
	})
	if err != nil {
		t.Fatalf("Apply(create) error = %v", err)
	}

	unchanged, err := registry.Apply(ApplyRequest{
		RequestToken:       "apply-same-payments",
		ExpectedGeneration: uint64Pointer(created.Resource.Generation),
		Resource:           desired(key, `{ "durability": "quorum", "max_attempts": 8 }`),
	})
	if err != nil {
		t.Fatalf("Apply(unchanged) error = %v", err)
	}
	if unchanged.Changed || unchanged.Resource.Generation != 1 {
		t.Fatalf("Apply(unchanged) = %+v", unchanged)
	}

	updated, err := registry.Apply(ApplyRequest{
		RequestToken:       "update-payments",
		ExpectedGeneration: uint64Pointer(1),
		Resource:           desired(key, `{"durability":"quorum","max_attempts":12}`),
	})
	if err != nil {
		t.Fatalf("Apply(update) error = %v", err)
	}
	if updated.Created || !updated.Changed || updated.Resource.Generation != 2 {
		t.Fatalf("Apply(update) = %+v", updated)
	}

	_, err = registry.Apply(ApplyRequest{
		RequestToken:       "stale-update-payments",
		ExpectedGeneration: uint64Pointer(1),
		Resource:           desired(key, `{"max_attempts":16}`),
	})
	var conflictError *RegistryError
	if !errors.As(err, &conflictError) || conflictError.Code != CodeConflict {
		t.Fatalf("Apply(stale) error = %v, want conflict", err)
	}
	if conflictError.ExpectedGeneration != 1 || conflictError.ActualGeneration != 2 {
		t.Fatalf("generation conflict = %+v", conflictError)
	}
}

func TestDeleteListAndRecreateKeepMonotonicGeneration(t *testing.T) {
	registry := NewRegistry()
	queueKey := ResourceKey{Namespace: "prod", Kind: KindQueue, Name: "jobs"}
	streamKey := ResourceKey{Namespace: "prod", Kind: KindStream, Name: "events"}
	devKey := ResourceKey{Namespace: "dev", Kind: KindQueue, Name: "jobs"}
	for index, key := range []ResourceKey{queueKey, streamKey, devKey} {
		_, err := registry.Apply(ApplyRequest{
			RequestToken: fmt.Sprintf("create-%d", index),
			Resource:     desired(key, `{}`),
		})
		if err != nil {
			t.Fatalf("Apply(%v) error = %v", key, err)
		}
	}

	listed, err := registry.List(ListFilter{Namespace: "prod"})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(listed) != 2 || listed[0].Kind != KindQueue || listed[1].Kind != KindStream {
		t.Fatalf("List(prod) = %+v", listed)
	}

	deleted, err := registry.Delete(DeleteRequest{
		RequestToken:       "delete-jobs",
		ExpectedGeneration: uint64Pointer(1),
		Key:                queueKey,
	})
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !deleted.Deleted || deleted.Replayed || deleted.Generation != 2 {
		t.Fatalf("Delete() = %+v", deleted)
	}
	replayed, err := registry.Delete(DeleteRequest{
		RequestToken:       "delete-jobs",
		ExpectedGeneration: uint64Pointer(1),
		Key:                queueKey,
	})
	if err != nil || !replayed.Replayed || replayed.Generation != 2 {
		t.Fatalf("Delete(replay) = %+v, %v", replayed, err)
	}
	if _, err := registry.Get(queueKey); err == nil {
		t.Fatal("Get(deleted) succeeded, want not_found")
	} else {
		assertCode(t, err, CodeNotFound)
	}

	recreated, err := registry.Apply(ApplyRequest{
		RequestToken:       "recreate-jobs",
		ExpectedGeneration: uint64Pointer(0),
		Resource:           desired(queueKey, `{"max_attempts":4}`),
	})
	if err != nil {
		t.Fatalf("Apply(recreate) error = %v", err)
	}
	if recreated.Resource.Generation != 3 {
		t.Fatalf("recreated generation = %d, want 3", recreated.Resource.Generation)
	}
}

func TestApplyDefensivelyCopiesInputsAndOutputs(t *testing.T) {
	registry := NewRegistry()
	labels := map[string]string{"owner": "payments"}
	spec := json.RawMessage(`{"max_attempts":8}`)
	result, err := registry.Apply(ApplyRequest{
		RequestToken: "copy-test",
		Resource: DesiredResource{
			ResourceKey: ResourceKey{Namespace: "prod", Kind: KindQueue, Name: "payments"},
			Labels:      labels,
			Spec:        spec,
		},
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	labels["owner"] = "mutated"
	spec[2] = 'X'
	result.Resource.Labels["owner"] = "also-mutated"
	result.Resource.Spec[2] = 'Y'

	stored, err := registry.Get(ResourceKey{Namespace: "prod", Kind: KindQueue, Name: "payments"})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Labels["owner"] != "payments" || string(stored.Spec) != `{"max_attempts":8}` {
		t.Fatalf("stored resource was aliased: %+v", stored)
	}
}

func TestConcurrentUnconditionalAppliesSerializeGenerations(t *testing.T) {
	registry := NewRegistry()
	key := ResourceKey{Namespace: "prod", Kind: KindCache, Name: "sessions"}
	const applies = 32
	var waitGroup sync.WaitGroup
	errorsChannel := make(chan error, applies)
	for index := 0; index < applies; index++ {
		waitGroup.Add(1)
		go func(value int) {
			defer waitGroup.Done()
			_, err := registry.Apply(ApplyRequest{
				RequestToken: fmt.Sprintf("concurrent-%d", value),
				Resource:     desired(key, fmt.Sprintf(`{"revision":%d}`, value)),
			})
			if err != nil {
				errorsChannel <- err
			}
		}(index)
	}
	waitGroup.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("Apply(concurrent) error = %v", err)
	}
	resource, err := registry.Get(key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if resource.Generation != applies {
		t.Fatalf("generation = %d, want %d", resource.Generation, applies)
	}
}

func TestApplyRejectsMalformedTrailingOrNonObjectSpec(t *testing.T) {
	registry := NewRegistry()
	key := ResourceKey{Namespace: "prod", Kind: KindCache, Name: "sessions"}
	for _, spec := range []string{`{"broken":`, `{} {}`, `[]`, `null`, `"scalar"`} {
		_, err := registry.Apply(ApplyRequest{
			RequestToken: "invalid-" + spec,
			Resource:     desired(key, spec),
		})
		assertCode(t, err, CodeInvalidArgument)
	}
}

func desired(key ResourceKey, spec string) DesiredResource {
	return DesiredResource{ResourceKey: key, Spec: json.RawMessage(spec)}
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}

func assertCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	var registryError *RegistryError
	if !errors.As(err, &registryError) {
		t.Fatalf("error = %v, want RegistryError(%s)", err, code)
	}
	if registryError.Code != code {
		t.Fatalf("error code = %s, want %s", registryError.Code, code)
	}
}
