package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// The protected cross-language step requires these executables explicitly.
// Ordinary Go-only developers need not install a JDK/Python for fixture units.
func TestPublicProbeMatrixKeepsTransportLossUnknown(t *testing.T) {
	if os.Getenv("EPOCH_MANAGEMENT_PROBE_MATRIX") != "1" {
		t.Skip("explicit three-language probe runtime required")
	}
	goProbe := os.Getenv("EPOCH_MANAGEMENT_GO_PROBE")
	python := os.Getenv("EPOCH_MANAGEMENT_PYTHON_EXECUTABLE")
	java := os.Getenv("EPOCH_MANAGEMENT_JAVA_EXECUTABLE")
	classpath := os.Getenv("EPOCH_MANAGEMENT_JAVA_CLASSPATH")
	if goProbe == "" || python == "" || java == "" || classpath == "" {
		t.Fatal("required Go/Python/Java probe executable or classpath missing")
	}
	pythonProbe, err := filepath.Abs("../management_sdk_python.py")
	if err != nil {
		t.Fatal(err)
	}
	for _, language := range []string{"go", "python", "java"} {
		t.Run(language, func(t *testing.T) {
			backend := &acknowledgedBackend{commits: make(chan *epochv1.BatchApplyResourcesRequest, 1)}
			upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			upstream := grpc.NewServer()
			epochv1.RegisterRegionalAdminServiceServer(upstream, backend)
			go func() { _ = upstream.Serve(upstreamListener) }()
			t.Cleanup(upstream.Stop)
			request := proxyRequest()
			request.RequestToken = "original-" + language
			proxy, err := newHeldProxy([]binding{{Request: request, Authority: upstreamListener.Addr().String()}}, "fixture-admin")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(proxy.close)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			epochv1.RegisterRegionalAdminServiceServer(server, proxy)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			plan := map[string]any{"schema": "epoch.sdk.management.plan/v1", "phase": "lost-ack-wire", "endpoints": []string{listener.Addr().String()}, "timeout_seconds": 15, "actions": []any{map[string]any{"method": "BatchApplyResources", "request_proto": encoded, "expected_code": 14, "expected_unknown": true}}}
			data, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "plan.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			var command *exec.Cmd
			switch language {
			case "go":
				command = exec.Command(goProbe, "--plan", path)
			case "python":
				command = exec.Command(python, pythonProbe, "--plan", path)
			case "java":
				command = exec.Command(java, "-cp", classpath, "io.epoch.sdk.ManagementCatalogProbe", "--plan", path)
			}
			command.Env = append(os.Environ(), "EPOCH_CONTROL_HA_GRPC_ADMIN_TOKEN=fixture-admin")
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			finished := make(chan error, 1)
			go func() { finished <- command.Wait() }()
			t.Cleanup(func() { _ = command.Process.Kill() })
			select {
			case observed := <-backend.commits:
				if !proto.Equal(observed, request) {
					t.Fatal("public probe changed original command")
				}
			case err := <-finished:
				t.Fatalf("probe exited before dispatch: %v; stderr=%s", err, stderr.String())
			case <-time.After(10 * time.Second):
				t.Fatal("public probe did not dispatch its planned command")
			}
			deadline := time.Now().Add(time.Second)
			for len(proxy.snapshot()) != 1 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if len(proxy.snapshot()) != 1 {
				t.Fatal("real upstream response not held")
			}
			select {
			case <-finished:
				t.Fatal("public probe completed before actual transport loss")
			case <-time.After(30 * time.Millisecond):
			}
			server.Stop()
			select {
			case err := <-finished:
				if err != nil {
					t.Fatalf("probe process failed: %v; stderr=%s", err, stderr.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("owned SDK probe did not finish after transport loss")
			}
			var proof struct {
				Schema   string `json:"schema"`
				Language string `json:"language"`
				Passed   bool   `json:"passed"`
				Actions  []struct {
					Code     int    `json:"grpc_code"`
					Attempts int    `json:"attempts"`
					Unknown  bool   `json:"outcome_may_be_unknown"`
					Request  []byte `json:"request_proto"`
				} `json:"actions"`
			}
			if err := json.Unmarshal(stdout.Bytes(), &proof); err != nil {
				t.Fatal(err)
			}
			if proof.Schema != "epoch.sdk.management.probe/v1" || proof.Language != language || !proof.Passed || len(proof.Actions) != 1 {
				t.Fatal("incomplete public SDK probe witness")
			}
			witness := proof.Actions[0]
			if witness.Code != 14 || witness.Attempts != 1 || !witness.Unknown || !bytes.Equal(witness.Request, encoded) {
				t.Fatal("transport-loss code, attempt, token, or original zero-presence bytes differ")
			}
			fmt.Printf("public %s probe: exact original command retained after actual socket loss\n", language)
		})
	}
}
