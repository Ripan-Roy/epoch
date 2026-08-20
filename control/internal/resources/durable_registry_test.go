package resources

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestDurableRegistryRecoversDesiredStatusTokensAndTombstones(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.db")
	key := ResourceKey{
		Organization: "acme",
		Project:      "shop",
		Environment:  "dev",
		Namespace:    "core",
		Kind:         KindStream,
		Name:         "orders",
	}
	apply := ApplyRequest{
		RequestToken:       "create-orders",
		ExpectedGeneration: uint64Pointer(0),
		Resource: DesiredResource{
			ResourceKey: key,
			Labels:      map[string]string{"owner": "checkout"},
			Governance: &ResourceGovernance{
				Owner:          "team:checkout",
				CostCenter:     "cc-1042",
				Classification: ClassificationConfidential,
				Tags:           map[string]string{"service": "checkout"},
			},
			Spec: json.RawMessage(`{"replica_count":3,"shard_count":1}`),
		},
	}

	first, err := OpenDurableRegistry(path)
	if err != nil {
		t.Fatalf("OpenDurableRegistry(first) error = %v", err)
	}
	created, err := first.Apply(apply)
	if err != nil {
		t.Fatalf("Apply(create) error = %v", err)
	}
	status := ResourceStatus{
		Phase:              PhaseReady,
		ObservedGeneration: created.Resource.Generation,
		Message:            "regional catalog and placement converged",
		Tablets: []TabletStatus{{
			TabletID:           7,
			ConsensusGroupID:   17,
			ShardIndex:         0,
			TabletEpoch:        3,
			ResourceGeneration: created.Resource.Generation,
			DesiredReplicas:    3,
			VoterNodeIDs:       []uint64{1, 2, 3},
			LeaderNodeID:       2,
		}},
	}
	if _, err := first.UpdateStatus(key, created.Resource.Generation, status); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}

	second, err := OpenDurableRegistry(path)
	if err != nil {
		t.Fatalf("OpenDurableRegistry(second) error = %v", err)
	}
	recovered, err := second.Get(key)
	if err != nil {
		t.Fatalf("Get(recovered) error = %v", err)
	}
	if recovered.Generation != 1 ||
		recovered.Labels["owner"] != "checkout" ||
		recovered.Governance == nil ||
		recovered.Governance.Owner != "team:checkout" ||
		recovered.Governance.Tags["service"] != "checkout" ||
		recovered.Status.Phase != PhaseReady ||
		recovered.Status.ObservedGeneration != 1 ||
		len(recovered.Status.Tablets) != 1 ||
		recovered.Status.Tablets[0].LeaderNodeID != 2 {
		t.Fatalf("recovered resource = %+v", recovered)
	}
	replayedApply, err := second.Apply(apply)
	if err != nil {
		t.Fatalf("Apply(replay after restart) error = %v", err)
	}
	if !replayedApply.Replayed || replayedApply.Resource.Generation != 1 {
		t.Fatalf("Apply(replay after restart) = %+v", replayedApply)
	}
	deleteRequest := DeleteRequest{
		RequestToken:       "delete-orders",
		ExpectedGeneration: uint64Pointer(1),
		Key:                key,
	}
	deleted, err := second.Delete(deleteRequest)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !deleted.Deleted || deleted.Generation != 2 {
		t.Fatalf("Delete() = %+v", deleted)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close(second) error = %v", err)
	}

	third, err := OpenDurableRegistry(path)
	if err != nil {
		t.Fatalf("OpenDurableRegistry(third) error = %v", err)
	}
	t.Cleanup(func() {
		if err := third.Close(); err != nil {
			t.Errorf("Close(third) error = %v", err)
		}
	})
	if _, err := third.Get(key); err == nil {
		t.Fatal("Get(deleted after restart) succeeded")
	} else {
		assertCode(t, err, CodeNotFound)
	}
	replayedDelete, err := third.Delete(deleteRequest)
	if err != nil {
		t.Fatalf("Delete(replay after restart) error = %v", err)
	}
	if !replayedDelete.Replayed || replayedDelete.Generation != 2 {
		t.Fatalf("Delete(replay after restart) = %+v", replayedDelete)
	}
	recreated, err := third.Apply(ApplyRequest{
		RequestToken:       "recreate-orders",
		ExpectedGeneration: uint64Pointer(0),
		Resource:           apply.Resource,
	})
	if err != nil {
		t.Fatalf("Apply(recreate after restart) error = %v", err)
	}
	if recreated.Resource.Generation != 3 {
		t.Fatalf("recreated generation = %d, want 3", recreated.Resource.Generation)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(registry) error = %v", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("registry permissions = %o, want no group/other access", info.Mode().Perm())
	}
	if third.Mode() != "bbolt_v1" {
		t.Fatalf("Mode() = %q, want bbolt_v1", third.Mode())
	}
}

func TestStoredLegacyRegionalResourceWithoutGovernanceRemainsReadable(t *testing.T) {
	key := ResourceKey{
		Organization: "acme", Project: "shop", Environment: "dev",
		Namespace: "core", Kind: KindStream, Name: "legacy-orders",
	}
	legacy := Resource{
		ResourceKey: key,
		Spec:        json.RawMessage(`{"replica_count":3,"shard_count":1}`),
		Generation:  1,
		Status: ResourceStatus{
			Phase: PhasePending,
		},
	}
	if err := validateStoredResource(key, legacy); err != nil {
		t.Fatalf("validateStoredResource(legacy) error = %v", err)
	}
}

func TestDurableRegistryFailsClosedForCorruptionAndExclusiveOwnership(t *testing.T) {
	directory := t.TempDir()
	corruptPath := filepath.Join(directory, "corrupt.db")
	if err := os.WriteFile(corruptPath, []byte("not a bbolt database"), 0o600); err != nil {
		t.Fatalf("WriteFile(corrupt) error = %v", err)
	}
	if registry, err := OpenDurableRegistry(corruptPath); err == nil {
		_ = registry.Close()
		t.Fatal("OpenDurableRegistry(corrupt) succeeded")
	}

	path := filepath.Join(directory, "exclusive.db")
	first, err := OpenDurableRegistry(path)
	if err != nil {
		t.Fatalf("OpenDurableRegistry(first) error = %v", err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Errorf("Close(first) error = %v", err)
		}
	})
	if second, err := OpenDurableRegistry(path); err == nil {
		_ = second.Close()
		t.Fatal("second OpenDurableRegistry() succeeded while first owns the file")
	}

	versionPath := filepath.Join(directory, "unknown-version.db")
	versioned, err := OpenDurableRegistry(versionPath)
	if err != nil {
		t.Fatalf("OpenDurableRegistry(versioned) error = %v", err)
	}
	if err := versioned.Close(); err != nil {
		t.Fatalf("Close(versioned) error = %v", err)
	}
	database, err := bolt.Open(versionPath, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open(versioned) error = %v", err)
	}
	if err := database.Update(func(transaction *bolt.Tx) error {
		return transaction.Bucket(metadataBucket).Put(
			schemaVersionKey,
			encodeSchemaVersion(durableSchemaVersion+1),
		)
	}); err != nil {
		_ = database.Close()
		t.Fatalf("write unknown schema version error = %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close(version database) error = %v", err)
	}
	if registry, err := OpenDurableRegistry(versionPath); err == nil {
		_ = registry.Close()
		t.Fatal("OpenDurableRegistry(unknown schema) succeeded")
	}
}

func TestRegistryDoesNotExposeFailedDurableMutation(t *testing.T) {
	persistence := &failingRegistryPersistence{commitError: errors.New("disk full")}
	registry := newRegistry(emptyRegistryState(), persistence)
	key := ResourceKey{Namespace: "prod", Kind: KindQueue, Name: "jobs"}
	request := ApplyRequest{
		RequestToken: "create-jobs",
		Resource:     desired(key, `{}`),
	}

	if _, err := registry.Apply(request); err == nil {
		t.Fatal("Apply() succeeded despite failed durable commit")
	} else {
		assertCode(t, err, CodeInternal)
	}
	if registry.Count() != 0 {
		t.Fatalf("Count() = %d after failed commit, want 0", registry.Count())
	}

	persistence.commitError = nil
	created, err := registry.Apply(request)
	if err != nil {
		t.Fatalf("Apply(retry) error = %v", err)
	}
	if created.Resource.Generation != 1 {
		t.Fatalf("generation after retry = %d, want 1", created.Resource.Generation)
	}

	persistence.commitError = errors.New("disk full")
	status := ResourceStatus{
		Phase:              PhaseReady,
		ObservedGeneration: created.Resource.Generation,
	}
	if _, err := registry.UpdateStatus(key, created.Resource.Generation, status); err == nil {
		t.Fatal("UpdateStatus() succeeded despite failed durable commit")
	} else {
		assertCode(t, err, CodeInternal)
	}
	stored, err := registry.Get(key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status.Phase != PhasePending {
		t.Fatalf("status after failed commit = %+v, want pending", stored.Status)
	}
}

type failingRegistryPersistence struct {
	commitError error
}

func (persistence *failingRegistryPersistence) Commit(registryMutation) error {
	return persistence.commitError
}

func (*failingRegistryPersistence) Close() error {
	return nil
}

func (*failingRegistryPersistence) Mode() string {
	return durableRegistryMode
}
