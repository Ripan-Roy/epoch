package epoch

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ManagementWatch is a single-consumer, explicitly acknowledged change watch.
// Process and durably checkpoint a page, then Acknowledge its NextCursor before
// Recv again. Reconnection uses only that scanned checkpoint. Context/Close
// controls the persistent stream lifetime; unary Timeout does not expire it.
type ManagementWatch struct {
	client       *ManagementClient
	ctx          context.Context
	cancel       context.CancelFunc
	request      *epochv1.WatchResourceChangesRequest
	stream       grpc.ServerStreamingClient[epochv1.WatchResourceChangesResponse]
	nextEndpoint int
	checkpoint   uint64
	pending      *uint64
	info         ManagementCallInfo
	closed       atomic.Bool
}

// WatchResourceChanges prepares a lazy stream over the configured allowlist.
// At most one stream is opened per endpoint during this handle's lifetime.
// Reopen explicitly from Checkpoint after exhaustion; it is never reset to zero.
func (client *ManagementClient) WatchResourceChanges(ctx context.Context, request *epochv1.WatchResourceChangesRequest) (*ManagementWatch, error) {
	if err := client.usable(); err != nil {
		return nil, err
	}
	if ctx == nil || request == nil || request.BatchSize > 1000 {
		return nil, managementInvalid("management watch requires a context and batch_size between 0 and 1000")
	}
	if request.Kind != 0 && epochv1.ResourceKind_name[int32(request.Kind)] == "" {
		return nil, managementInvalid("management watch resource kind is unknown")
	}
	ctx, cancel := context.WithCancel(client.authenticated(ctx))
	return &ManagementWatch{client: client, ctx: ctx, cancel: cancel,
		request: proto.Clone(request).(*epochv1.WatchResourceChangesRequest), checkpoint: request.AfterCursor,
		info: ManagementCallInfo{Budget: len(client.clients)},
	}, nil
}

// Close cancels the stream and may be called concurrently with the consumer.
func (watch *ManagementWatch) Close() {
	if watch != nil && watch.closed.CompareAndSwap(false, true) {
		watch.cancel()
	}
}

// Checkpoint returns the last acknowledged scanned cursor, not a pending page.
func (watch *ManagementWatch) Checkpoint() uint64 { return watch.checkpoint }

// Info returns the cumulative SDK stream-attempt count and fixed endpoint budget.
func (watch *ManagementWatch) Info() ManagementCallInfo { return watch.info }

// Acknowledge advances the checkpoint only to the exact pending page NextCursor.
func (watch *ManagementWatch) Acknowledge(cursor uint64) error {
	if watch.closed.Load() {
		return status.Error(codes.Canceled, "management watch is closed")
	}
	if watch.pending == nil {
		return status.Error(codes.FailedPrecondition, "management watch has no pending page")
	}
	if cursor != *watch.pending {
		return managementInvalid("acknowledge the exact scanned NextCursor")
	}
	watch.checkpoint, watch.pending = cursor, nil
	return nil
}

// Recv returns one validated page and requires acknowledgement before advancing.
func (watch *ManagementWatch) Recv() (*epochv1.WatchResourceChangesResponse, error) {
	if watch.closed.Load() {
		return nil, status.Error(codes.Canceled, "management watch is closed")
	}
	if watch.pending != nil {
		return nil, status.Error(codes.FailedPrecondition, "acknowledge the current page before receiving another")
	}
	var failure error
	for {
		if err := watch.ctx.Err(); err != nil {
			return watch.fail(err)
		}
		if err := watch.client.usable(); err != nil {
			return watch.fail(err)
		}
		if watch.stream == nil {
			if watch.nextEndpoint == len(watch.client.clients) {
				return watch.fail(failure)
			}
			request := proto.Clone(watch.request).(*epochv1.WatchResourceChangesRequest)
			request.AfterCursor = watch.checkpoint
			endpoint := watch.client.clients[watch.nextEndpoint]
			watch.nextEndpoint++
			watch.info.Attempts++
			stream, err := endpoint.WatchResourceChanges(watch.ctx, request)
			if err == nil && stream == nil {
				err = status.Error(codes.DataLoss, "management watch stream is missing")
			}
			if err != nil {
				failure = err
				if status.Code(err) == codes.Unavailable {
					continue
				}
				return watch.fail(err)
			}
			watch.stream = stream
		}
		page, err := watch.stream.Recv()
		if err != nil {
			watch.stream, failure = nil, err
			if status.Code(err) == codes.Unavailable || errors.Is(err, io.EOF) {
				continue
			}
			return watch.fail(err)
		}
		if err := validateManagementWatchPage(watch.request, watch.checkpoint, page); err != nil {
			return watch.fail(err)
		}
		cursor := page.NextCursor
		watch.pending = &cursor
		return page, nil
	}
}

func (watch *ManagementWatch) fail(err error) (*epochv1.WatchResourceChangesResponse, error) {
	watch.Close()
	if err == nil {
		err = status.Error(codes.Unavailable, "management watch exhausted its endpoint budget")
	}
	return nil, &ManagementRPCError{Info: watch.info, cause: err}
}

func validateManagementWatchPage(filter *epochv1.WatchResourceChangesRequest, after uint64, page *epochv1.WatchResourceChangesResponse) error {
	invalid := func() error {
		return status.Error(codes.DataLoss, "management watch page violates its scope or scanned-cursor contract")
	}
	if page == nil || page.EarliestCursor == 0 || page.EarliestCursor-1 > after || (page.EarliestCursor > page.LatestCursor && !(page.LatestCursor == 0 && page.EarliestCursor == 1)) || page.NextCursor < after || page.NextCursor > page.LatestCursor {
		return invalid()
	}
	limit := filter.BatchSize
	if limit == 0 {
		limit = 100
	}
	if len(page.Changes) > int(limit) {
		return invalid()
	}
	previous := after
	for _, change := range page.Changes {
		if change == nil || change.Cursor <= previous || change.Cursor > page.NextCursor || change.Generation == 0 || change.Kind < epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_DESIRED_APPLIED || change.Kind > epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_STATUS_UPDATED || validateManagementName(change.Name) != nil {
			return invalid()
		}
		name := change.Name
		if (filter.Organization != "" && filter.Organization != name.Organization) ||
			(filter.Project != "" && filter.Project != name.Project) ||
			(filter.Environment != "" && filter.Environment != name.Environment) ||
			(filter.Namespace != "" && filter.Namespace != name.Namespace) ||
			(filter.Kind != 0 && filter.Kind != name.Kind) {
			return invalid()
		}
		previous = change.Cursor
	}
	return nil
}
