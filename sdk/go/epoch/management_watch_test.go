package epoch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type managementStreamStub struct {
	grpc.ClientStream
	pages   []*epochv1.WatchResourceChangesResponse
	failure error
}

func (stream *managementStreamStub) Recv() (*epochv1.WatchResourceChangesResponse, error) {
	if len(stream.pages) == 0 {
		if stream.failure != nil {
			return nil, stream.failure
		}
		return nil, io.EOF
	}
	page := stream.pages[0]
	stream.pages = stream.pages[1:]
	return page, nil
}

type managementWatchStub struct {
	epochv1.RegionalAdminServiceClient
	watchCall func(context.Context, *epochv1.WatchResourceChangesRequest) (grpc.ServerStreamingClient[epochv1.WatchResourceChangesResponse], error)
}

func (stub *managementWatchStub) WatchResourceChanges(ctx context.Context, request *epochv1.WatchResourceChangesRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[epochv1.WatchResourceChangesResponse], error) {
	return stub.watchCall(ctx, request)
}

func TestManagementWatchResumesOnlyTheAcknowledgedScannedCursor(t *testing.T) {
	request := &epochv1.WatchResourceChangesRequest{AfterCursor: 10, BatchSize: 2, Organization: "acme", Project: "shop", Environment: "qa", Namespace: "core"}
	original := proto.Clone(request).(*epochv1.WatchResourceChangesRequest)
	page := &epochv1.WatchResourceChangesResponse{EarliestCursor: 1, LatestCursor: 20, NextCursor: 13,
		Changes: []*epochv1.ResourceChange{{Cursor: 11, Name: managementTestName(), Generation: 1, Kind: epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_DESIRED_APPLIED}},
	}
	second := &epochv1.WatchResourceChangesResponse{EarliestCursor: 1, LatestCursor: 20, NextCursor: 16}
	firstCalls, secondCalls := 0, 0
	first := &managementWatchStub{watchCall: func(_ context.Context, got *epochv1.WatchResourceChangesRequest) (grpc.ServerStreamingClient[epochv1.WatchResourceChangesResponse], error) {
		firstCalls++
		if !proto.Equal(got, original) {
			t.Fatal("initial watch request changed")
		}
		return &managementStreamStub{pages: []*epochv1.WatchResourceChangesResponse{page}, failure: status.Error(codes.Unavailable, "controller lost")}, nil
	}}
	last := &managementWatchStub{watchCall: func(_ context.Context, got *epochv1.WatchResourceChangesRequest) (grpc.ServerStreamingClient[epochv1.WatchResourceChangesResponse], error) {
		secondCalls++
		want := proto.Clone(original).(*epochv1.WatchResourceChangesRequest)
		want.AfterCursor = 13 // Not last visible 11, latest 20, or unacknowledged 16.
		if !proto.Equal(got, want) {
			t.Fatal("watch failover lost the exact filter or scanned checkpoint")
		}
		return &managementStreamStub{pages: []*epochv1.WatchResourceChangesResponse{second}}, nil
	}}
	client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{first, last}, token: "sdk-secret", timeout: time.Second}
	watch, err := client.WatchResourceChanges(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	got, err := watch.Recv()
	if err != nil || !proto.Equal(got, page) || watch.Checkpoint() != 10 {
		t.Fatalf("first page = %v, %v", got, err)
	}
	if _, err := watch.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("watch advanced an unacknowledged page")
	}
	if err := watch.Acknowledge(11); status.Code(err) != codes.InvalidArgument {
		t.Fatal("last visible cursor was accepted instead of scanned cursor")
	}
	if err := watch.Acknowledge(13); err != nil {
		t.Fatal(err)
	}
	got, err = watch.Recv()
	if err != nil || !proto.Equal(got, second) || watch.Checkpoint() != 13 || watch.Info().Attempts != 2 {
		t.Fatalf("resumed page = %v, %v", got, err)
	}
	if err := watch.Acknowledge(16); err != nil || watch.Checkpoint() != 16 {
		t.Fatal("empty filtered page lost its scanned cursor")
	}
	if firstCalls != 1 || secondCalls != 1 || !proto.Equal(request, original) {
		t.Fatal("watch retried unboundedly or modified caller state")
	}
}

func TestManagementWatchFailsClosedOnStaleCursorAndMalformedPages(t *testing.T) {
	for label, mutate := range map[string]func(*epochv1.WatchResourceChangesResponse){
		"lost retained prefix": func(page *epochv1.WatchResourceChangesResponse) { page.EarliestCursor = 13; page.Changes = nil },
		"cursor regression":    func(page *epochv1.WatchResourceChangesResponse) { page.NextCursor = 9 },
		"future cursor":        func(page *epochv1.WatchResourceChangesResponse) { page.NextCursor = 21 },
		"foreign scope":        func(page *epochv1.WatchResourceChangesResponse) { page.Changes[0].Name.Organization = "otherco" },
		"duplicate event":      func(page *epochv1.WatchResourceChangesResponse) { page.Changes = append(page.Changes, page.Changes[0]) },
		"unscanned event":      func(page *epochv1.WatchResourceChangesResponse) { page.Changes[0].Cursor = 14 },
		"unknown change":       func(page *epochv1.WatchResourceChangesResponse) { page.Changes[0].Kind = 99 },
	} {
		t.Run(label, func(t *testing.T) {
			page := &epochv1.WatchResourceChangesResponse{EarliestCursor: 1, LatestCursor: 20, NextCursor: 13, Changes: []*epochv1.ResourceChange{{Cursor: 11, Name: managementTestName(), Generation: 1, Kind: epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_DESIRED_APPLIED}}}
			mutate(page)
			stub := &managementWatchStub{watchCall: func(context.Context, *epochv1.WatchResourceChangesRequest) (grpc.ServerStreamingClient[epochv1.WatchResourceChangesResponse], error) {
				return &managementStreamStub{pages: []*epochv1.WatchResourceChangesResponse{page}}, nil
			}}
			client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{stub}, token: "secret", timeout: time.Second}
			watch, err := client.WatchResourceChanges(context.Background(), &epochv1.WatchResourceChangesRequest{AfterCursor: 10, Organization: "acme"})
			if err != nil {
				t.Fatal(err)
			}
			defer watch.Close()
			if _, err := watch.Recv(); status.Code(err) != codes.DataLoss || watch.Checkpoint() != 10 {
				t.Fatal("malformed page advanced durable checkpoint")
			}
		})
	}
	calls := 0
	stub := &managementWatchStub{watchCall: func(context.Context, *epochv1.WatchResourceChangesRequest) (grpc.ServerStreamingClient[epochv1.WatchResourceChangesResponse], error) {
		calls++
		return &managementStreamStub{failure: status.Error(codes.Aborted, "stale cursor")}, nil
	}}
	client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{stub, stub}, token: "secret", timeout: time.Second}
	watch, err := client.WatchResourceChanges(context.Background(), &epochv1.WatchResourceChangesRequest{AfterCursor: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	if _, err := watch.Recv(); status.Code(err) != codes.Aborted || calls != 1 || watch.Checkpoint() != 10 {
		t.Fatal("stale cursor was retried or silently reset")
	}
}

func TestManagementWatchEndpointBudgetIsNeverResetAfterEOF(t *testing.T) {
	calls := 0
	stub := &managementWatchStub{watchCall: func(ctx context.Context, request *epochv1.WatchResourceChangesRequest) (grpc.ServerStreamingClient[epochv1.WatchResourceChangesResponse], error) {
		calls++
		if _, exists := ctx.Deadline(); exists {
			t.Fatal("unary timeout leaked into the persistent watch")
		}
		if request.AfterCursor != 17 {
			t.Fatal("watch silently reset the caller checkpoint")
		}
		return &managementStreamStub{}, nil
	}}
	client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{stub, stub}, token: "secret", timeout: time.Nanosecond}
	watch, err := client.WatchResourceChanges(context.Background(), &epochv1.WatchResourceChangesRequest{AfterCursor: 17})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	if page, err := watch.Recv(); page != nil || !errors.Is(err, io.EOF) || calls != 2 || watch.Info().Attempts != 2 || watch.Info().Budget != 2 || watch.Checkpoint() != 17 {
		t.Fatalf("watch exceeded its endpoint budget: calls=%d, info=%+v, err=%v", calls, watch.Info(), err)
	}
	if _, err := watch.Recv(); status.Code(err) != codes.Canceled || calls != 2 {
		t.Fatal("an exhausted watch opened additional streams")
	}
}

type managementBlockingStreamStub struct {
	grpc.ClientStream
	ctx     context.Context
	started chan struct{}
}

func (stream *managementBlockingStreamStub) Recv() (*epochv1.WatchResourceChangesResponse, error) {
	close(stream.started)
	<-stream.ctx.Done()
	return nil, stream.ctx.Err()
}

func TestManagementWatchCancellationDoesNotOpenAnotherController(t *testing.T) {
	for _, closeHandle := range []bool{false, true} {
		t.Run(fmt.Sprintf("close=%v", closeHandle), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			calls := 0
			stub := &managementWatchStub{watchCall: func(ctx context.Context, _ *epochv1.WatchResourceChangesRequest) (grpc.ServerStreamingClient[epochv1.WatchResourceChangesResponse], error) {
				calls++
				return &managementBlockingStreamStub{ctx: ctx, started: started}, nil
			}}
			client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{stub, stub}, token: "secret", timeout: time.Second}
			watch, err := client.WatchResourceChanges(ctx, &epochv1.WatchResourceChangesRequest{AfterCursor: 17})
			if err != nil {
				t.Fatal(err)
			}
			defer watch.Close()
			result := make(chan error, 1)
			go func() { _, err := watch.Recv(); result <- err }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("watch did not begin receiving")
			}
			if closeHandle {
				watch.Close()
				watch.Close()
			} else {
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) || status.Code(err) != codes.Canceled || calls != 1 || watch.Info().Attempts != 1 || watch.Checkpoint() != 17 {
					t.Fatalf("cancellation changed checkpoint or opened another stream: calls=%d, info=%+v, err=%v", calls, watch.Info(), err)
				}
			case <-time.After(time.Second):
				t.Fatal("watch cancellation did not unblock Recv")
			}
		})
	}
}
