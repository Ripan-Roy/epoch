package main

import (
	"context"
	"net"
	"testing"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type acknowledgedBackend struct {
	epochv1.UnimplementedRegionalAdminServiceServer
	commits chan *epochv1.BatchApplyResourcesRequest
}

func (backend *acknowledgedBackend) BatchApplyResources(ctx context.Context, request *epochv1.BatchApplyResourcesRequest) (*epochv1.BatchApplyResourcesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer fixture-admin" {
		return nil, status.Error(codes.Unauthenticated, "fixture identity required")
	}
	backend.commits <- proto.Clone(request).(*epochv1.BatchApplyResourcesRequest)
	item := request.Resources[0]
	return &epochv1.BatchApplyResourcesResponse{Results: []*epochv1.ApplyResourceResponse{{Resource: &epochv1.Resource{Name: item.Name, Spec: item.Spec, Generation: 1}, Created: true, Changed: true}}}, nil
}

func proxyRequest() *epochv1.BatchApplyResourcesRequest {
	zero := uint64(0)
	return &epochv1.BatchApplyResourcesRequest{RequestToken: "original-token", Resources: []*epochv1.BatchApplyResource{{
		Name: &epochv1.ResourceName{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core", Kind: epochv1.ResourceKind_RESOURCE_KIND_CACHE, Name: "owned-sdk-test"},
		Spec: &epochv1.ResourceSpec{Replicas: 3}, ExpectedGeneration: &zero,
	}}}
}

func TestProxyHoldsTheActualResponseAndTransportLossStaysUnknown(t *testing.T) {
	backend := &acknowledgedBackend{commits: make(chan *epochv1.BatchApplyResourcesRequest, 2)}
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream := grpc.NewServer()
	epochv1.RegisterRegionalAdminServiceServer(upstream, backend)
	go func() { _ = upstream.Serve(upstreamListener) }()
	t.Cleanup(upstream.Stop)
	request := proxyRequest()
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
	client, err := epoch.NewManagementClient(epoch.ManagementConfig{Endpoints: []string{listener.Addr().String()}, BearerToken: "fixture-admin", Timeout: 5 * time.Second, AllowInsecureLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	finished := make(chan error, 1)
	go func() {
		_, _, failure := client.BatchApplyResources(context.Background(), request)
		finished <- failure
	}()
	select {
	case received := <-backend.commits:
		if !proto.Equal(received, request) {
			t.Fatal("upstream command changed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no forwarded command")
	}
	deadline := time.Now().Add(time.Second)
	for len(proxy.snapshot()) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(proxy.snapshot()) != 1 {
		t.Fatal("upstream acknowledgement not witnessed")
	}
	select {
	case <-finished:
		t.Fatal("response escaped before transport fault")
	case <-time.After(30 * time.Millisecond):
	}
	server.Stop()
	select {
	case failure := <-finished:
		rpc, ok := failure.(*epoch.ManagementRPCError)
		if !ok || rpc.GRPCStatus().Code() != codes.Unavailable || !rpc.Info.OutcomeMayBeUnknown || rpc.Info.Attempts != 1 {
			t.Fatalf("unexpected transport-loss evidence: %v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("owned transport did not release caller")
	}
}

func TestProxyRejectsForeignCommandsAndIdentityBeforeUpstreamDispatch(t *testing.T) {
	request := proxyRequest()
	proxy, err := newHeldProxy([]binding{{Request: request, Authority: "127.0.0.1:12345"}}, "fixture-admin")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.close)
	for _, test := range []struct {
		token  string
		mutate func(*epochv1.BatchApplyResourcesRequest)
		code   codes.Code
	}{
		{"wrong", func(*epochv1.BatchApplyResourcesRequest) {}, codes.Unauthenticated},
		{"fixture-admin", func(value *epochv1.BatchApplyResourcesRequest) { value.Resources[0].ExpectedGeneration = nil }, codes.InvalidArgument},
		{"fixture-admin", func(value *epochv1.BatchApplyResourcesRequest) { value.RequestToken = "replacement" }, codes.InvalidArgument},
	} {
		value := proto.Clone(request).(*epochv1.BatchApplyResourcesRequest)
		test.mutate(value)
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+test.token))
		_, err := proxy.BatchApplyResources(ctx, value)
		if status.Code(err) != test.code {
			t.Fatalf("code=%v want=%v", status.Code(err), test.code)
		}
	}
	if len(proxy.snapshot()) != 0 {
		t.Fatal("rejected request fabricated a commit witness")
	}
}

func TestProxyPlanRequiresBoundedUniqueTokensAndLoopbackAuthorities(t *testing.T) {
	for _, bindings := range [][]binding{
		nil,
		{{Request: nil, Authority: "127.0.0.1:12345"}},
		{{Request: proxyRequest(), Authority: "example.com:12345"}},
		{{Request: proxyRequest(), Authority: "127.0.0.1:12345"}, {Request: proxyRequest(), Authority: "127.0.0.1:12346"}},
	} {
		proxy, err := newHeldProxy(bindings, "fixture-admin")
		if err == nil {
			proxy.close()
			t.Fatal("invalid proxy plan accepted")
		}
	}
}
