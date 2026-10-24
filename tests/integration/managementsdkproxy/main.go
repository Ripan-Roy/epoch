// managementsdkproxy is a private loopback-only lost-ack fixture, not an Epoch
// gateway. It forwards exact planned public-SDK batches and holds real upstream
// responses. The fault driver kills this process to produce actual socket loss.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const planSchema = "epoch.sdk.management.lost-ack-plan/v1"

type binding struct {
	Request   *epochv1.BatchApplyResourcesRequest
	Authority string
}

type heldReceipt struct {
	RequestToken      string `json:"request_token"`
	RequestProto      []byte `json:"request_proto"`
	ResponseProto     []byte `json:"response_proto"`
	UpstreamAuthority string `json:"upstream_authority"`
}

type forward struct {
	request    *epochv1.BatchApplyResourcesRequest
	client     epochv1.RegionalAdminServiceClient
	authority  string
	dispatched bool
}

type heldProxy struct {
	epochv1.UnimplementedRegionalAdminServiceServer
	lock        sync.Mutex
	token       string
	bindings    map[string]*forward
	connections []*grpc.ClientConn
	receipts    []heldReceipt
}

func loopbackAuthority(authority string) bool {
	host, port, err := net.SplitHostPort(authority)
	number, portErr := strconv.Atoi(port)
	return err == nil && portErr == nil && number > 0 && number <= 65535 && strconv.Itoa(number) == port && (host == "localhost" || net.ParseIP(host).IsLoopback())
}

func newHeldProxy(bindings []binding, token string) (*heldProxy, error) {
	if token == "" || len(bindings) < 1 || len(bindings) > 8 {
		return nil, errors.New("explicit identity and one to eight exact bindings required")
	}
	seen := map[string]bool{}
	for _, item := range bindings {
		request := item.Request
		if !loopbackAuthority(item.Authority) || request == nil || request.RequestToken == "" || len(request.RequestToken) > 256 || request.RequestToken != strings.TrimSpace(request.RequestToken) || seen[request.RequestToken] || len(request.Resources) < 1 || len(request.Resources) > 128 || proto.Size(request) > 512<<10 {
			return nil, errors.New("invalid, duplicate, oversized, or non-loopback binding")
		}
		for _, resource := range request.Resources {
			if resource == nil || resource.Name == nil || resource.Spec == nil {
				return nil, errors.New("binding requires complete resource identity and spec")
			}
		}
		seen[request.RequestToken] = true
	}
	proxy := &heldProxy{token: token, bindings: make(map[string]*forward)}
	for _, item := range bindings {
		connection, err := grpc.NewClient("passthrough:///"+item.Authority, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithNoProxy(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(5<<20), grpc.MaxCallSendMsgSize(5<<20)))
		if err != nil {
			proxy.close()
			return nil, err
		}
		proxy.connections = append(proxy.connections, connection)
		proxy.bindings[item.Request.RequestToken] = &forward{request: proto.Clone(item.Request).(*epochv1.BatchApplyResourcesRequest), client: epochv1.NewRegionalAdminServiceClient(connection), authority: item.Authority}
	}
	return proxy, nil
}

func (proxy *heldProxy) close() {
	for _, connection := range proxy.connections {
		_ = connection.Close()
	}
}

func (proxy *heldProxy) snapshot() []heldReceipt {
	proxy.lock.Lock()
	defer proxy.lock.Unlock()
	// Return independent immutable byte witnesses, never a mutable live slice.
	result := make([]heldReceipt, len(proxy.receipts))
	for index, receipt := range proxy.receipts {
		result[index] = receipt
		result[index].RequestProto = append([]byte(nil), receipt.RequestProto...)
		result[index].ResponseProto = append([]byte(nil), receipt.ResponseProto...)
	}
	return result
}

func (proxy *heldProxy) BatchApplyResources(ctx context.Context, request *epochv1.BatchApplyResourcesRequest) (*epochv1.BatchApplyResourcesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer "+proxy.token {
		return nil, status.Error(codes.Unauthenticated, "fixture identity required")
	}
	proxy.lock.Lock()
	bound := proxy.bindings[request.GetRequestToken()]
	if bound == nil || !proto.Equal(bound.request, request) || bound.dispatched {
		proxy.lock.Unlock()
		return nil, status.Error(codes.InvalidArgument, "only the exact original planned command may be forwarded once")
	}
	bound.dispatched = true
	proxy.lock.Unlock()
	upstream := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+proxy.token))
	response, err := bound.client.BatchApplyResources(upstream, request)
	if err != nil {
		return nil, err
	}
	if response == nil || len(response.Results) != len(request.Resources) {
		return nil, status.Error(codes.DataLoss, "upstream omitted the complete batch receipt")
	}
	encodedRequest, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	encodedResponse, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	if err != nil {
		return nil, err
	}
	proxy.lock.Lock()
	proxy.receipts = append(proxy.receipts, heldReceipt{RequestToken: request.RequestToken, RequestProto: encodedRequest, ResponseProto: encodedResponse, UpstreamAuthority: bound.authority})
	proxy.lock.Unlock()
	// Do not send headers, a receipt, or a fabricated failure. The driver first
	// verifies these real acknowledgements, stops the Catalog leader with all
	// SDK callers pending, then SIGKILLs this owned process to sever transport.
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (proxy *heldProxy) serveSnapshot(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || request.URL.Path != "/held" {
		http.NotFound(response, request)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(struct {
		Schema   string        `json:"schema"`
		Receipts []heldReceipt `json:"receipts"`
	}{"epoch.sdk.management.held-receipts/v1", proxy.snapshot()})
}

func loadBindings(path string) ([]binding, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 4<<20 {
		return nil, errors.New("proxy plan exceeds its byte budget")
	}
	var document struct {
		Schema   string `json:"schema"`
		Bindings []struct {
			Request   []byte `json:"request_proto"`
			Authority string `json:"upstream_authority"`
		} `json:"bindings"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if document.Schema != planSchema {
		return nil, errors.New("wrong exact-command proxy plan schema")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("proxy plan must contain one bounded JSON object")
	}
	bindings := make([]binding, 0, len(document.Bindings))
	for _, item := range document.Bindings {
		request := &epochv1.BatchApplyResourcesRequest{}
		if err := proto.Unmarshal(item.Request, request); err != nil {
			return nil, err
		}
		bindings = append(bindings, binding{Request: request, Authority: item.Authority})
	}
	return bindings, nil
}

func run(plan, ready string) error {
	bindings, err := loadBindings(plan)
	if err != nil {
		return err
	}
	proxy, err := newHeldProxy(bindings, os.Getenv("EPOCH_CONTROL_HA_GRPC_ADMIN_TOKEN"))
	if err != nil {
		return err
	}
	defer proxy.close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	statusListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() { _ = statusListener.Close() }()
	server := grpc.NewServer(grpc.MaxRecvMsgSize(5<<20), grpc.MaxSendMsgSize(5<<20))
	epochv1.RegisterRegionalAdminServiceServer(server, proxy)
	defer server.Stop()
	httpServer := &http.Server{Handler: http.HandlerFunc(proxy.serveSnapshot), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second}
	defer func() { _ = httpServer.Close() }()
	failures := make(chan error, 2)
	go func() { failures <- server.Serve(listener) }()
	go func() { failures <- httpServer.Serve(statusListener) }()
	data, err := json.Marshal(struct {
		Schema    string `json:"schema"`
		Authority string `json:"grpc_authority"`
		StatusURL string `json:"status_url"`
	}{"epoch.sdk.management.proxy-ready/v1", listener.Addr().String(), "http://" + statusListener.Addr().String() + "/held"})
	if err != nil {
		return err
	}
	file, err := os.OpenFile(ready, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		return nil
	case err := <-failures:
		return err
	}
}

func main() {
	plan := flag.String("plan", "", "owned exact original-command plan")
	ready := flag.String("ready", "", "new owned readiness artifact")
	flag.Parse()
	if *plan == "" || *ready == "" {
		fmt.Fprintln(os.Stderr, "explicit plan and new readiness path required")
		os.Exit(1)
	}
	if err := run(*plan, *ready); err != nil {
		fmt.Fprintln(os.Stderr, "management lost-ack fixture failed:", err)
		os.Exit(1)
	}
}
