package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const maxRequestIDBytes = 128

// NewUnaryServerInterceptor authenticates every gRPC call before it can reach
// a managed-control handler. Method-specific authorization remains in the
// service so it can evaluate the parsed tenant scope before any state access.
func NewUnaryServerInterceptor(
	policy *Policy,
	audit AuditSink,
) grpc.UnaryServerInterceptor {
	if policy == nil {
		panic("auth: nil gRPC policy")
	}
	if audit == nil {
		panic("auth: nil gRPC audit sink")
	}
	return func(
		ctx context.Context,
		request any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		incoming, _ := metadata.FromIncomingContext(ctx)
		requestID := requestIDFromMetadata(incoming)
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-request-id", requestID))
		authorizationValues := incoming.Get("authorization")
		authorization := ""
		if len(authorizationValues) == 1 {
			authorization = authorizationValues[0]
		} else if len(authorizationValues) > 1 {
			authorization = "malformed"
		}
		principal, err := policy.AuthenticateBearer(authorization)
		if err != nil {
			audit.Record(ctx, DecisionEvent{
				Timestamp:   time.Now().UTC(),
				RequestID:   requestID,
				PrincipalID: "anonymous",
				PolicyID:    policy.ID(),
				Action:      actionForGRPCMethod(info.FullMethod),
				Decision:    DecisionDeny,
				Reason:      authenticationReason(err),
				Scope:       Scope{},
			})
			return nil, status.Error(
				codes.Unauthenticated,
				"valid bearer authentication is required",
			)
		}
		ctx = ContextWithPrincipal(ctx, principal)
		ctx = ContextWithRequestID(ctx, requestID)
		return handler(ctx, request)
	}
}

func authenticationReason(err error) DecisionReason {
	var authenticationError *AuthenticationError
	if !errors.As(err, &authenticationError) {
		return ReasonInvalidCredential
	}
	switch authenticationError.Kind {
	case AuthenticationMissing:
		return ReasonMissingCredential
	case AuthenticationMalformed:
		return ReasonMalformedCredential
	default:
		return ReasonInvalidCredential
	}
}

func actionForGRPCMethod(method string) Action {
	switch method {
	case "/epoch.v1.RegionalAdminService/ApplyResource":
		return ActionResourceApply
	case "/epoch.v1.RegionalAdminService/DeleteResource":
		return ActionResourceDelete
	default:
		return ActionResourceRead
	}
}

func requestIDFromMetadata(incoming metadata.MD) string {
	values := incoming.Get("x-request-id")
	if len(values) == 1 && validRequestID(values[0]) {
		return values[0]
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err == nil {
		return "request-" + hex.EncodeToString(random[:])
	}
	return fmt.Sprintf("request-%d", time.Now().UnixNano())
}

func validRequestID(candidate string) bool {
	if candidate == "" || len(candidate) > maxRequestIDBytes {
		return false
	}
	for _, character := range candidate {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}
