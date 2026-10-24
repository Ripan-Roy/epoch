package epoch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ManagementConfig selects an explicit controller allowlist and trust policy.
// TLS is mandatory unless AllowInsecureLoopback explicitly selects local-only
// development. Timeout bounds the entire unary failover sequence, not each hop.
type ManagementConfig struct {
	Endpoints             []string
	BearerToken           string
	Timeout               time.Duration
	TLS                   *TLSConfig
	AllowInsecureLoopback bool
}

// ManagementCallInfo counts SDK-level RPC attempts, not physical network frames.
// An unsuccessful mutation with OutcomeMayBeUnknown must retain its original
// request token and scope for resolution; cancellation is not rollback.
type ManagementCallInfo struct {
	Attempts            int
	Budget              int
	OutcomeMayBeUnknown bool
}

// ManagementRPCError preserves the original gRPC status/details and cause.
// Error omits backend text; GRPCStatus and Unwrap expose it deliberately.
type ManagementRPCError struct {
	Info  ManagementCallInfo
	cause error
}

func (failure *ManagementRPCError) Error() string {
	return fmt.Sprintf("epoch: management RPC failed after %d attempts (%s)", failure.Info.Attempts, failure.GRPCStatus().Code())
}

func (failure *ManagementRPCError) Unwrap() error { return failure.cause }

func (failure *ManagementRPCError) GRPCStatus() *status.Status {
	if errors.Is(failure.cause, context.Canceled) || errors.Is(failure.cause, context.DeadlineExceeded) {
		return status.FromContextError(failure.cause)
	}
	return status.Convert(failure.cause)
}

// ManagementClient uses generated, presence-aware RegionalAdmin messages.
// Connections and unary calls are safe for concurrent use. Close cancels
// connection activity and is idempotent. Watch handles have a single consumer.
type ManagementClient struct {
	clients     []epochv1.RegionalAdminServiceClient
	connections []*grpc.ClientConn
	token       string
	timeout     time.Duration
	closed      atomic.Bool
}

// NewManagementClient validates every target and trust input before creating
// lazy generated gRPC connections. Call Close when the client is no longer used.
func NewManagementClient(config ManagementConfig) (*ManagementClient, error) {
	if len(config.Endpoints) == 0 || config.Timeout <= 0 {
		return nil, status.Error(codes.InvalidArgument, "management endpoints and a positive timeout are required")
	}
	if !validManagementBearer(config.BearerToken) {
		return nil, status.Error(codes.InvalidArgument, "management bearer token must be a bounded visible-ASCII credential")
	}
	if (config.TLS == nil && !config.AllowInsecureLoopback) || (config.TLS != nil && config.AllowInsecureLoopback) {
		return nil, status.Error(codes.InvalidArgument, "choose explicit TLS trust or explicit insecure loopback development")
	}
	seen := map[string]bool{}
	for _, endpoint := range config.Endpoints {
		host, port, err := net.SplitHostPort(endpoint)
		portNumber, portErr := strconv.Atoi(port)
		if err != nil || host == "" || strings.ContainsAny(host, "/\\@?# \t\r\n") || portErr != nil || portNumber < 1 || portNumber > 65535 || strconv.Itoa(portNumber) != port || seen[endpoint] {
			return nil, status.Error(codes.InvalidArgument, "management endpoints must be distinct host:port authorities")
		}
		seen[endpoint] = true
		if config.AllowInsecureLoopback && host != "localhost" && !net.ParseIP(host).IsLoopback() {
			return nil, status.Error(codes.InvalidArgument, "plaintext management endpoints must be loopback")
		}
	}
	var trust credentials.TransportCredentials = insecure.NewCredentials()
	if config.TLS != nil {
		loaded, err := loadClientTLS(*config.TLS)
		if err != nil {
			return nil, err
		}
		trust = credentials.NewTLS(loaded)
	}
	client := &ManagementClient{token: config.BearerToken, timeout: config.Timeout}
	for _, endpoint := range config.Endpoints {
		connection, err := grpc.NewClient("passthrough:///"+endpoint,
			grpc.WithTransportCredentials(trust.Clone()), grpc.WithDisableRetry(),
			grpc.WithDisableServiceConfig(), grpc.WithNoProxy(), grpc.WithUserAgent(userAgent),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(5<<20), grpc.MaxCallSendMsgSize(5<<20)))
		if err != nil {
			_ = client.Close()
			return nil, err
		}
		client.connections = append(client.connections, connection)
		client.clients = append(client.clients, epochv1.NewRegionalAdminServiceClient(connection))
	}
	return client, nil
}

func validManagementBearer(token string) bool {
	if len(token) == 0 || len(token) > 4096 {
		return false
	}
	for _, value := range []byte(token) {
		if value < 33 || value > 126 {
			return false
		}
	}
	return true
}

// Close idempotently closes all owned connections, including active streams.
func (client *ManagementClient) Close() error {
	if client == nil || !client.closed.CompareAndSwap(false, true) {
		return nil
	}
	var failures []error
	for _, connection := range client.connections {
		failures = append(failures, connection.Close())
	}
	return errors.Join(failures...)
}

func (client *ManagementClient) usable() error {
	if client == nil || client.closed.Load() || len(client.clients) == 0 {
		return status.Error(codes.Unavailable, "management client is closed or unconfigured")
	}
	return nil
}

func (client *ManagementClient) authenticated(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	if md == nil {
		md = metadata.MD{}
	} else {
		md = md.Copy()
	}
	md.Set("authorization", "Bearer "+client.token)
	return metadata.NewOutgoingContext(ctx, md)
}

func managementCall[Q proto.Message, R proto.Message](ctx context.Context, client *ManagementClient, request Q, mutation bool, call func(context.Context, epochv1.RegionalAdminServiceClient, Q) (R, error), validate func(Q, R) error) (R, ManagementCallInfo, error) {
	var empty R
	info := ManagementCallInfo{}
	if err := client.usable(); err != nil {
		return empty, info, err
	}
	info.Budget = len(client.clients)
	if ctx == nil {
		return empty, info, status.Error(codes.InvalidArgument, "management context is required")
	}
	ctx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	ctx = client.authenticated(ctx)
	frozen := proto.Clone(request).(Q)
	var failure error
	for _, endpoint := range client.clients {
		if err := ctx.Err(); err != nil {
			failure = err
			break
		}
		if err := client.usable(); err != nil {
			failure = err
			break
		}
		info.Attempts++
		response, err := call(ctx, endpoint, proto.Clone(frozen).(Q))
		if err == nil {
			if response.ProtoReflect().IsValid() {
				err = validate(frozen, response)
				if err == nil {
					info.OutcomeMayBeUnknown = false
					return response, info, nil
				}
			} else {
				err = status.Error(codes.DataLoss, "management response is missing")
			}
		}
		failure = err
		code := status.Code(err)
		// A gRPC code alone is not commit evidence. Apply can accept desired
		// state and then fail reconciliation; definitive outcome details are
		// not yet part of this provisional service. Resolve the original token
		// rather than claiming a semantic error proves non-commit.
		if mutation {
			info.OutcomeMayBeUnknown = true
		}
		if code != codes.Unavailable {
			break
		}
	}
	return empty, info, &ManagementRPCError{Info: info, cause: failure}
}
