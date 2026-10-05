package main

import (
	"context"
	"net"
	"testing"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type echoManagement struct {
	epochv1.UnimplementedRegionalAdminServiceServer
	requests chan *epochv1.BatchApplyResourcesRequest
}

func (service *echoManagement) BatchApplyResources(_ context.Context, request *epochv1.BatchApplyResourcesRequest) (*epochv1.BatchApplyResourcesResponse, error) {
	service.requests <- proto.Clone(request).(*epochv1.BatchApplyResourcesRequest)
	return &epochv1.BatchApplyResourcesResponse{Results: []*epochv1.ApplyResourceResponse{{Resource: &epochv1.Resource{Name: request.Resources[0].Name, Spec: request.Resources[0].Spec, Generation: 1}, Created: true, Changed: true}}}, nil
}

func TestGoProbeUsesThePublicClientWithoutChangingPresenceOrToken(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service := &echoManagement{requests: make(chan *epochv1.BatchApplyResourcesRequest, 1)}
	server := grpc.NewServer()
	epochv1.RegisterRegionalAdminServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client, err := epoch.NewManagementClient(epoch.ManagementConfig{Endpoints: []string{listener.Addr().String()}, BearerToken: "fixture-admin", Timeout: time.Second, AllowInsecureLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	zero := uint64(0)
	request := &epochv1.BatchApplyResourcesRequest{RequestToken: "original-token", Resources: []*epochv1.BatchApplyResource{{Name: &epochv1.ResourceName{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core", Kind: epochv1.ResourceKind_RESOURCE_KIND_CACHE, Name: "owned"}, Spec: &epochv1.ResourceSpec{Replicas: 3}, ExpectedGeneration: &zero}}}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := invoke(context.Background(), client, action{Method: "BatchApplyResources", Request: data})
	if err != nil {
		t.Fatal(err)
	}
	if witness.Code != 0 || witness.Attempts != 1 || witness.Unknown || len(witness.Response) == 0 {
		t.Fatalf("wrong public SDK receipt: %+v", witness)
	}
	if !proto.Equal(<-service.requests, request) {
		t.Fatal("original command was replaced")
	}
}

func TestGoProbeRejectsUnknownMethodsAndMalformedWireBeforeDispatch(t *testing.T) {
	client, err := epoch.NewManagementClient(epoch.ManagementConfig{Endpoints: []string{"127.0.0.1:12345"}, BearerToken: "fixture-admin", Timeout: time.Second, AllowInsecureLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	for _, item := range []action{{Method: "UnplannedMethod"}, {Method: "BatchApplyResources", Request: []byte{0xff}}} {
		if _, err := invoke(context.Background(), client, item); err == nil {
			t.Fatal("malformed probe action accepted")
		}
	}
}
