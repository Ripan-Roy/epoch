// Public Go SDK probe for the owned Catalog fault campaign. Protobuf requests
// come from a checksum-bound plan; this helper never generates replacement tokens.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type action struct {
	Method          string `json:"method"`
	Request         []byte `json:"request_proto"`
	ExpectedCode    int    `json:"expected_code"`
	ExpectedUnknown *bool  `json:"expected_unknown,omitempty"`
	CheckpointPath  string `json:"checkpoint_path,omitempty"`
	ReadyPath       string `json:"ready_path,omitempty"`
	ReleasePath     string `json:"release_path,omitempty"`
}

type actionProof struct {
	Method     string   `json:"method"`
	Request    []byte   `json:"request_proto"`
	Response   []byte   `json:"response_proto,omitempty"`
	Pages      [][]byte `json:"pages_proto,omitempty"`
	Code       int      `json:"grpc_code"`
	Attempts   int      `json:"attempts"`
	Budget     int      `json:"budget"`
	Unknown    bool     `json:"outcome_may_be_unknown"`
	Checkpoint string   `json:"checkpoint,omitempty"`
}

type probePlan struct {
	Schema         string   `json:"schema"`
	Phase          string   `json:"phase"`
	Endpoints      []string `json:"endpoints"`
	TimeoutSeconds int      `json:"timeout_seconds"`
	Actions        []action `json:"actions"`
}

type probeProof struct {
	Schema   string        `json:"schema"`
	Language string        `json:"language"`
	Phase    string        `json:"phase"`
	Passed   bool          `json:"passed"`
	Actions  []actionProof `json:"actions"`
}

func durableJSON(path string, value any) error {
	if path == "" {
		return errors.New("explicit owned checkpoint path required")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".sdk-checkpoint-")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

func invoke(ctx context.Context, client *epoch.ManagementClient, item action) (actionProof, error) {
	witness := actionProof{Method: item.Method, Request: item.Request}
	if item.Method == "WatchResourceChanges" || item.Method == "WatchReconnect" {
		return invokeWatch(ctx, client, item)
	}
	var request proto.Message
	switch item.Method {
	case "ApplyResource":
		request = &epochv1.ApplyResourceRequest{}
	case "BatchApplyResources":
		request = &epochv1.BatchApplyResourcesRequest{}
	case "DeleteResource":
		request = &epochv1.DeleteResourceRequest{}
	case "GetResource":
		request = &epochv1.GetResourceRequest{}
	case "GetOperation":
		request = &epochv1.GetOperationRequest{}
	case "ListResources":
		request = &epochv1.ListResourcesRequest{}
	default:
		return witness, errors.New("unplanned management method")
	}
	if err := proto.Unmarshal(item.Request, request); err != nil {
		return witness, err
	}
	var response proto.Message
	var info epoch.ManagementCallInfo
	var failure error
	switch typed := request.(type) {
	case *epochv1.ApplyResourceRequest:
		response, info, failure = client.ApplyResource(ctx, typed)
	case *epochv1.BatchApplyResourcesRequest:
		response, info, failure = client.BatchApplyResources(ctx, typed)
	case *epochv1.DeleteResourceRequest:
		response, info, failure = client.DeleteResource(ctx, typed)
	case *epochv1.GetResourceRequest:
		response, info, failure = client.GetResource(ctx, typed)
	case *epochv1.GetOperationRequest:
		response, info, failure = client.GetOperation(ctx, typed)
	case *epochv1.ListResourcesRequest:
		response, info, failure = client.ListResources(ctx, typed)
	}
	witness.Code, witness.Attempts, witness.Budget, witness.Unknown = int(status.Code(failure)), info.Attempts, info.Budget, info.OutcomeMayBeUnknown
	if failure == nil {
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
		if err != nil {
			return witness, err
		}
		witness.Response = encoded
	}
	return witness, nil
}

func invokeWatch(ctx context.Context, client *epoch.ManagementClient, item action) (actionProof, error) {
	witness := actionProof{Method: item.Method, Request: item.Request}
	request := &epochv1.WatchResourceChangesRequest{}
	if err := proto.Unmarshal(item.Request, request); err != nil {
		return witness, err
	}
	watch, failure := client.WatchResourceChanges(ctx, request)
	if failure == nil {
		defer watch.Close()
		target := uint64(0)
		for index := range 512 {
			page, err := watch.Recv()
			if err != nil {
				failure = err
				break
			}
			if index == 0 {
				target = page.LatestCursor
			}
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(page)
			if err != nil {
				return witness, err
			}
			checkpoint := strconv.FormatUint(page.NextCursor, 10)
			// Application progress is made durable before ACK, including empty
			// filtered pages. No business side effect is claimed by this fixture.
			if err := durableJSON(item.CheckpointPath, map[string]string{"schema": "epoch.sdk.management.checkpoint/v1", "language": "go", "next_cursor": checkpoint}); err != nil {
				return witness, err
			}
			if err := watch.Acknowledge(page.NextCursor); err != nil {
				return witness, err
			}
			witness.Pages = append(witness.Pages, encoded)
			witness.Checkpoint = strconv.FormatUint(watch.Checkpoint(), 10)
			info := watch.Info()
			witness.Attempts, witness.Budget = info.Attempts, info.Budget
			if item.Method == "WatchReconnect" && index == 0 {
				if err := durableJSON(item.ReadyPath, witness); err != nil {
					return witness, err
				}
				if item.ReleasePath == "" {
					return witness, errors.New("explicit owned release path required")
				}
				for {
					if _, err := os.Stat(item.ReleasePath); err == nil {
						break
					} else if !os.IsNotExist(err) {
						return witness, err
					}
					select {
					case <-ctx.Done():
						return witness, ctx.Err()
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			if item.Method == "WatchReconnect" && info.Attempts > 1 || item.Method == "WatchResourceChanges" && page.NextCursor >= target {
				return witness, nil
			}
		}
		if failure == nil {
			return witness, errors.New("watch exceeded its bounded page budget")
		}
	}
	witness.Code = int(status.Code(failure))
	var rpc *epoch.ManagementRPCError
	if errors.As(failure, &rpc) {
		witness.Attempts, witness.Budget, witness.Unknown = rpc.Info.Attempts, rpc.Info.Budget, rpc.Info.OutcomeMayBeUnknown
	}
	return witness, nil
}

func run(path string) (probeProof, error) {
	proof := probeProof{Schema: "epoch.sdk.management.probe/v1", Language: "go"}
	file, err := os.Open(path)
	if err != nil {
		return proof, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, (8<<20)+1))
	if err != nil || len(data) > 8<<20 {
		return proof, errors.New("unreadable or oversized SDK plan")
	}
	var plan probePlan
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return proof, err
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF || plan.Schema != "epoch.sdk.management.plan/v1" || plan.Phase == "" || len(plan.Actions) < 1 || len(plan.Actions) > 512 || plan.TimeoutSeconds < 1 || plan.TimeoutSeconds > 120 {
		return proof, errors.New("invalid bounded SDK plan")
	}
	proof.Phase = plan.Phase
	client, err := epoch.NewManagementClient(epoch.ManagementConfig{Endpoints: plan.Endpoints, BearerToken: os.Getenv("EPOCH_CONTROL_HA_GRPC_ADMIN_TOKEN"), Timeout: time.Duration(plan.TimeoutSeconds) * time.Second, AllowInsecureLoopback: true})
	if err != nil {
		return proof, err
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	for _, item := range plan.Actions {
		witness, err := invoke(ctx, client, item)
		proof.Actions = append(proof.Actions, witness)
		if err != nil {
			return proof, err
		}
		if item.ExpectedCode < 0 || item.ExpectedCode > int(codes.Unauthenticated) || witness.Code != item.ExpectedCode || item.ExpectedUnknown != nil && witness.Unknown != *item.ExpectedUnknown {
			return proof, fmt.Errorf("%s: expected code/unknown outcome differs from public SDK witness", item.Method)
		}
	}
	proof.Passed = true
	return proof, nil
}

func main() {
	path := flag.String("plan", "", "owned checksum-bound public SDK workload")
	flag.Parse()
	proof, err := run(*path)
	if encoding := json.NewEncoder(os.Stdout).Encode(proof); encoding != nil {
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "public Go SDK probe failed:", err)
		os.Exit(1)
	}
}
