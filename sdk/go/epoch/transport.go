package epoch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	userAgent        = "epoch-go/0.1.0-alpha.2"
	maxResponseBytes = 16 << 20
)

// Request is the transport-neutral representation of one Epoch call.
type Request struct {
	Method string
	Path   string
	Query  url.Values
	Body   any
}

// Transport is the narrow dependency consumed by Client.
type Transport interface {
	Do(context.Context, Request, any) error
}

// HTTPTransport uses net/http and performs no automatic retries.
type HTTPTransport struct {
	baseURL string
	client  *http.Client
}

// NewHTTPTransport validates a node URL and constructs a bounded HTTP client.
func NewHTTPTransport(baseURL string, timeout time.Duration) (*HTTPTransport, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("epoch: parse base URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("epoch: base URL must be an absolute HTTP(S) URL without query or fragment")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("epoch: timeout must be greater than zero")
	}
	return &HTTPTransport{
		baseURL: strings.TrimRight(parsed.String(), "/"),
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Do sends one JSON request and decodes one JSON response.
func (transport *HTTPTransport) Do(ctx context.Context, request Request, result any) error {
	method := strings.ToUpper(strings.TrimSpace(request.Method))
	if method == "" {
		return fmt.Errorf("epoch: request method is required")
	}
	path, err := normalizeRequestPath(request.Path)
	if err != nil {
		return err
	}
	requestURL := transport.baseURL + path
	if encoded := request.Query.Encode(); encoded != "" {
		requestURL += "?" + encoded
	}

	var body io.Reader
	if request.Body != nil {
		payload, marshalErr := json.Marshal(request.Body)
		if marshalErr != nil {
			return fmt.Errorf("epoch: encode request body: %w", marshalErr)
		}
		body = bytes.NewReader(payload)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return fmt.Errorf("epoch: build request: %w", err)
	}
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("User-Agent", userAgent)
	if request.Body != nil {
		httpRequest.Header.Set("Content-Type", "application/json")
	}

	response, err := transport.client.Do(httpRequest)
	if err != nil {
		return &APIError{
			StatusCode: 0,
			Code:       "transport_error",
			Detail:     err.Error(),
			cause:      err,
		}
	}
	defer response.Body.Close()
	payload, err := readBounded(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return decodeSuccess(payload, result)
	}
	return decodeAPIError(response.StatusCode, payload)
}

func normalizeRequestPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("epoch: request path is required")
	}
	if strings.ContainsAny(path, "?#") {
		return "", fmt.Errorf("epoch: request path cannot contain a query or fragment")
	}
	if strings.HasPrefix(path, "/") {
		return path, nil
	}
	return "/" + path, nil
}

func readBounded(reader io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("epoch: read response body: %w", err)
	}
	if len(payload) > maxResponseBytes {
		return nil, fmt.Errorf("epoch: response body exceeds %d bytes", maxResponseBytes)
	}
	return payload, nil
}

func decodeSuccess(payload []byte, result any) error {
	if len(bytes.TrimSpace(payload)) == 0 {
		return nil
	}
	if result == nil {
		var discarded any
		result = &discarded
	}
	if err := json.Unmarshal(payload, result); err != nil {
		return fmt.Errorf("epoch: response contained invalid JSON: %w", err)
	}
	return nil
}

func decodeAPIError(statusCode int, payload []byte) error {
	failure := &APIError{
		StatusCode: statusCode,
		Code:       "http_error",
		Detail:     http.StatusText(statusCode),
		Body:       append(json.RawMessage(nil), payload...),
	}
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Detail  string `json:"detail"`
			Message string `json:"message"`
		} `json:"error"`
		Code    string `json:"code"`
		Detail  string `json:"detail"`
		Message string `json:"message"`
	}
	if json.Unmarshal(payload, &envelope) == nil {
		if envelope.Error.Code != "" || envelope.Error.Detail != "" || envelope.Error.Message != "" {
			failure.Code = firstNonEmpty(envelope.Error.Code, failure.Code)
			failure.Detail = firstNonEmpty(envelope.Error.Detail, envelope.Error.Message, failure.Detail)
		} else {
			failure.Code = firstNonEmpty(envelope.Code, failure.Code)
			failure.Detail = firstNonEmpty(envelope.Detail, envelope.Message, failure.Detail)
		}
	}
	if failure.Detail == "" {
		failure.Detail = fmt.Sprintf("HTTP %d", statusCode)
	}
	return failure
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
