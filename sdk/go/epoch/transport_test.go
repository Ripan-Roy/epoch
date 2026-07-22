package epoch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestHTTPTransportSendsJSONAndDecodesSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "POST" || request.URL.Path != "/v1/streams/orders/records" {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
		}
		if request.URL.Query().Get("partition") != "1" {
			t.Errorf("unexpected query: %s", request.URL.RawQuery)
		}
		if request.Header.Get("User-Agent") != "epoch-go/0.1.0-alpha.1" {
			t.Errorf("unexpected user agent: %s", request.Header.Get("User-Agent"))
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		if string(body) != `{"value":"ok"}` {
			t.Errorf("unexpected body: %s", body)
		}
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(`{"offset":7}`))
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	var result struct {
		Offset uint64 `json:"offset"`
	}
	err = transport.Do(context.Background(), Request{
		Method: "POST",
		Path:   "/v1/streams/orders/records",
		Query:  url.Values{"partition": {"1"}},
		Body:   Document{"value": "ok"},
	}, &result)
	if err != nil {
		t.Fatalf("Do returned an error: %v", err)
	}
	if result.Offset != 7 {
		t.Fatalf("unexpected response: %#v", result)
	}
}

func TestHTTPTransportAcceptsEmptySuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	var result Document
	if err := transport.Do(context.Background(), Request{Method: "DELETE", Path: "/resource"}, &result); err != nil {
		t.Fatalf("Do returned an error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %#v", result)
	}
}

func TestHTTPTransportReturnsStructuredAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("content-type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte(`{"error":{"code":"unavailable","detail":"try later"}}`))
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	err = transport.Do(context.Background(), Request{Method: "GET", Path: "/resource"}, nil)
	var apiError *APIError
	if !errors.As(err, &apiError) {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if apiError.StatusCode != 503 || apiError.Code != "unavailable" || apiError.Detail != "try later" {
		t.Fatalf("unexpected API error: %#v", apiError)
	}
	if !apiError.Retryable() {
		t.Fatal("503 should be classified as retryable")
	}
	var decoded map[string]any
	if err := json.Unmarshal(apiError.Body, &decoded); err != nil {
		t.Fatalf("error body was not preserved: %v", err)
	}
}

func TestHTTPTransportPreservesNonJSONProxyError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusBadGateway)
		_, _ = writer.Write([]byte("upstream reset"))
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	err = transport.Do(context.Background(), Request{Method: "GET", Path: "/resource"}, nil)
	var apiError *APIError
	if !errors.As(err, &apiError) {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if apiError.StatusCode != 502 || apiError.Code != "http_error" {
		t.Fatalf("unexpected API error: %#v", apiError)
	}
	if strings.TrimSpace(string(apiError.Body)) != "upstream reset" {
		t.Fatalf("proxy body was not preserved: %q", apiError.Body)
	}
}

func TestHTTPTransportRejectsInvalidSuccessJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("not-json"))
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	var result Document
	err = transport.Do(context.Background(), Request{Method: "GET", Path: "/resource"}, &result)
	if err == nil || !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("expected invalid JSON error, got %v", err)
	}
}

func TestHTTPTransportDoesNotFollowRedirects(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected = true
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	err = transport.Do(context.Background(), Request{Method: "POST", Path: "/mutation", Body: Document{"value": 1}}, nil)
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("expected redirect APIError, got %T: %v", err, err)
	}
	if redirected {
		t.Fatal("transport followed a redirect")
	}
}
