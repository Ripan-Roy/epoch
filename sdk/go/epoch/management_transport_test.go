package epoch

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"strings"
	"testing"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type managementWireServer struct {
	epochv1.UnimplementedRegionalAdminServiceServer
	requests    chan proto.Message
	watchClosed chan struct{}
}

func (server *managementWireServer) ApplyResource(_ context.Context, request *epochv1.ApplyResourceRequest) (*epochv1.ApplyResourceResponse, error) {
	server.requests <- request
	return &epochv1.ApplyResourceResponse{Resource: &epochv1.Resource{Name: request.Name, Spec: request.Spec, Generation: 1}, Created: true, Changed: true}, nil
}
func (server *managementWireServer) GetResource(_ context.Context, request *epochv1.GetResourceRequest) (*epochv1.GetResourceResponse, error) {
	server.requests <- request
	return &epochv1.GetResourceResponse{Resource: &epochv1.Resource{Name: request.Name, Spec: &epochv1.ResourceSpec{}, Generation: 1}}, nil
}
func (server *managementWireServer) ListResources(_ context.Context, request *epochv1.ListResourcesRequest) (*epochv1.ListResourcesResponse, error) {
	server.requests <- request
	return &epochv1.ListResourcesResponse{Resources: []*epochv1.Resource{{Name: managementTestName(), Spec: &epochv1.ResourceSpec{Governance: &epochv1.ResourceGovernance{Tags: request.Tags}}, Generation: 1}}, NextPageToken: "opaque-next-page"}, nil
}
func (server *managementWireServer) DeleteResource(_ context.Context, request *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
	server.requests <- request
	return &epochv1.DeleteResourceResponse{Name: request.Name, Generation: ^uint64(0), Replayed: true}, nil
}
func (server *managementWireServer) BatchApplyResources(_ context.Context, request *epochv1.BatchApplyResourcesRequest) (*epochv1.BatchApplyResourcesResponse, error) {
	server.requests <- request
	response := &epochv1.BatchApplyResourcesResponse{Replayed: true}
	for _, item := range request.Resources {
		response.Results = append(response.Results, &epochv1.ApplyResourceResponse{Resource: &epochv1.Resource{Name: item.Name, Spec: item.Spec, Generation: 1}, Created: true, Changed: true, Replayed: true})
	}
	return response, nil
}
func (server *managementWireServer) GetOperation(_ context.Context, request *epochv1.GetOperationRequest) (*epochv1.GetOperationResponse, error) {
	server.requests <- request
	return &epochv1.GetOperationResponse{RequestToken: request.RequestToken, ProposalId: ^uint64(0), AffectedResources: request.AffectedResources, State: epochv1.OperationState_OPERATION_STATE_SUCCEEDED, CommandKind: "delete_managed", ExpectedGeneration: proto.Uint64(0)}, nil
}
func (server *managementWireServer) WatchResourceChanges(request *epochv1.WatchResourceChangesRequest, stream grpc.ServerStreamingServer[epochv1.WatchResourceChangesResponse]) error {
	if err := authorizeManagementWireContext(stream.Context()); err != nil {
		return err
	}
	server.requests <- request
	if err := stream.Send(&epochv1.WatchResourceChangesResponse{EarliestCursor: 1, LatestCursor: 20, NextCursor: 13, Changes: []*epochv1.ResourceChange{{Cursor: 11, Name: managementTestName(), Generation: 1, Kind: epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_DESIRED_APPLIED}}}); err != nil {
		return err
	}
	<-stream.Context().Done()
	close(server.watchClosed)
	return stream.Context().Err()
}

func authorizeManagementWireContext(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || values[0] != "Bearer sdk-secret" {
		return status.Error(codes.Unauthenticated, "invalid identity")
	}
	remote, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing transport identity")
	}
	identity, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || identity.State.Version != tls.VersionTLS13 || len(identity.State.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "verified TLS client identity required")
	}
	return nil
}

func TestManagementGeneratedCallsRunOverMutualTLS(t *testing.T) {
	material := generateMutualTLSMaterial(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(material.caPEM)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{material.serverIdentity}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})),
		grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if err := authorizeManagementWireContext(ctx); err != nil {
				return nil, err
			}
			return next(ctx, request)
		}))
	implementation := &managementWireServer{requests: make(chan proto.Message, 16), watchClosed: make(chan struct{})}
	epochv1.RegisterRegionalAdminServiceServer(server, implementation)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:9")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9")
	config := ManagementConfig{Endpoints: []string{listener.Addr().String()}, BearerToken: "sdk-secret", Timeout: 2 * time.Second,
		TLS: &TLSConfig{RootCAPath: material.caPath, CertificatePath: material.clientCertificatePath, PrivateKeyPath: material.clientPrivateKeyPath},
	}
	client, err := NewManagementClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	name := managementTestName()
	apply := &epochv1.ApplyResourceRequest{RequestToken: "apply-token", Name: name, Spec: &epochv1.ResourceSpec{WorkloadProfile: epochv1.WorkloadProfile_WORKLOAD_PROFILE_CACHE, Durability: epochv1.DurabilityProfile_DURABILITY_PROFILE_QUORUM_DURABLE, Replicas: 3, Labels: map[string]string{"revision": "sdk-wire-test"}}, ExpectedGeneration: proto.Uint64(0)}
	if result, info, err := client.ApplyResource(ctx, apply); err != nil || result.GetResource().GetGeneration() != 1 || info.Attempts != 1 {
		t.Fatalf("apply: %+v, %v", info, err)
	}
	assertManagementWireRequest(t, implementation.requests, apply)
	get := &epochv1.GetResourceRequest{Name: name}
	if result, _, err := client.GetResource(ctx, get); err != nil || !proto.Equal(result.GetResource().GetName(), name) {
		t.Fatalf("get: %v", err)
	}
	assertManagementWireRequest(t, implementation.requests, get)
	list := &epochv1.ListResourcesRequest{Organization: "acme", PageSize: 50, PageToken: "opaque-page", Tags: map[string]string{"cost": "team"}}
	if result, _, err := client.ListResources(ctx, list); err != nil || result.GetNextPageToken() != "opaque-next-page" {
		t.Fatalf("list: %v", err)
	}
	assertManagementWireRequest(t, implementation.requests, list)
	deleteRequest := &epochv1.DeleteResourceRequest{RequestToken: "delete-token", Name: name, ExpectedGeneration: proto.Uint64(0)}
	if result, _, err := client.DeleteResource(ctx, deleteRequest); err != nil || result.GetGeneration() != ^uint64(0) {
		t.Fatalf("delete: %v", err)
	}
	assertManagementWireRequest(t, implementation.requests, deleteRequest)
	batch := &epochv1.BatchApplyResourcesRequest{RequestToken: "batch-token", Resources: []*epochv1.BatchApplyResource{{Name: name, Spec: apply.Spec, ExpectedGeneration: proto.Uint64(0)}}}
	if result, _, err := client.BatchApplyResources(ctx, batch); err != nil || !result.GetReplayed() {
		t.Fatalf("batch: %v", err)
	}
	assertManagementWireRequest(t, implementation.requests, batch)
	operation := &epochv1.GetOperationRequest{RequestToken: "delete-token", AffectedResources: []*epochv1.ResourceName{name}}
	if result, _, err := client.GetOperation(ctx, operation); err != nil || result.GetProposalId() != ^uint64(0) || result.ExpectedGeneration == nil || *result.ExpectedGeneration != 0 {
		t.Fatalf("operation: %v", err)
	}
	assertManagementWireRequest(t, implementation.requests, operation)
	watchRequest := &epochv1.WatchResourceChangesRequest{AfterCursor: 10, BatchSize: 2, Organization: "acme"}
	watch, err := client.WatchResourceChanges(ctx, watchRequest)
	if err != nil {
		t.Fatal(err)
	}
	if page, err := watch.Recv(); err != nil || page.GetNextCursor() != 13 {
		t.Fatalf("watch: %v", err)
	}
	assertManagementWireRequest(t, implementation.requests, watchRequest)
	if err := watch.Acknowledge(13); err != nil {
		t.Fatal(err)
	}
	watch.Close()
	select {
	case <-implementation.watchClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("watch Close did not cancel the remote stream")
	}
	if _, err := watch.Recv(); status.Code(err) != codes.Canceled {
		t.Fatal("closed watch still received")
	}
	config.BearerToken = "wrong-secret"
	unauthorized, err := NewManagementClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer unauthorized.Close()
	if _, info, err := unauthorized.GetResource(ctx, get); status.Code(err) != codes.Unauthenticated || info.Attempts != 1 || strings.Contains(err.Error(), "wrong-secret") {
		t.Fatalf("authentication failure: %+v, %v", info, err)
	}
	config.BearerToken = "sdk-secret"
	config.TLS = &TLSConfig{RootCAPath: material.caPath}
	anonymous, err := NewManagementClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer anonymous.Close()
	if _, _, err := anonymous.GetResource(ctx, get); err == nil {
		t.Fatal("mTLS accepted an anonymous client")
	}
}

func assertManagementWireRequest(t *testing.T, requests <-chan proto.Message, expected proto.Message) {
	t.Helper()
	select {
	case actual := <-requests:
		if !proto.Equal(actual, expected) {
			t.Fatalf("generated wire request changed: want %v, got %v", expected, actual)
		}
	case <-time.After(time.Second):
		t.Fatal("generated method was not called")
	}
}
