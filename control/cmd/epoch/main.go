// Command epoch is the user-facing management CLI for declarative Epoch resources.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sigs.k8s.io/yaml"
)

const (
	defaultTimeout = 15 * time.Second
	maxInputSize   = 1 << 20
)

type cliConfig struct {
	grpcEndpoint string
	httpEndpoint string
	token        string
	timeout      time.Duration
}

type adminClient interface {
	ApplyResource(context.Context, *epochv1.ApplyResourceRequest, ...grpc.CallOption) (*epochv1.ApplyResourceResponse, error)
	GetResource(context.Context, *epochv1.GetResourceRequest, ...grpc.CallOption) (*epochv1.GetResourceResponse, error)
	ListResources(context.Context, *epochv1.ListResourcesRequest, ...grpc.CallOption) (*epochv1.ListResourcesResponse, error)
	DeleteResource(context.Context, *epochv1.DeleteResourceRequest, ...grpc.CallOption) (*epochv1.DeleteResourceResponse, error)
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "epoch:", err)
		os.Exit(1)
	}
}

func run(arguments []string, stdin io.Reader, stdout, stderr io.Writer) error {
	global := flag.NewFlagSet("epoch", flag.ContinueOnError)
	global.SetOutput(stderr)
	config := cliConfig{}
	global.StringVar(&config.grpcEndpoint, "endpoint", envOr("EPOCH_CONTROL_ENDPOINT", "127.0.0.1:8081"), "RegionalAdmin gRPC endpoint")
	global.StringVar(&config.httpEndpoint, "http-endpoint", envOr("EPOCH_CONTROL_HTTP_ENDPOINT", "http://127.0.0.1:8080"), "control HTTP endpoint")
	global.StringVar(&config.token, "token", os.Getenv("EPOCH_TOKEN"), "bearer token (or EPOCH_TOKEN)")
	global.DurationVar(&config.timeout, "timeout", defaultTimeout, "operation timeout")
	global.Usage = func() { printUsage(stderr) }
	if err := global.Parse(arguments); err != nil {
		return err
	}
	remaining := global.Args()
	if len(remaining) == 0 {
		printUsage(stderr)
		return errors.New("a command is required")
	}
	if remaining[0] == "help" || remaining[0] == "--help" || remaining[0] == "-h" {
		printUsage(stdout)
		return nil
	}
	if strings.TrimSpace(config.token) == "" {
		return errors.New("--token or EPOCH_TOKEN is required")
	}
	if config.timeout <= 0 || config.timeout > 5*time.Minute {
		return errors.New("--timeout must be between 1ns and 5m")
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.timeout)
	defer cancel()
	connection, err := grpc.NewClient(
		config.grpcEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(bearerInterceptor(config.token)),
	)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", config.grpcEndpoint, err)
	}
	defer connection.Close()
	client := epochv1.NewRegionalAdminServiceClient(connection)
	return execute(ctx, client, config, remaining, stdin, stdout, stderr)
}

func execute(
	ctx context.Context,
	client adminClient,
	config cliConfig,
	arguments []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
) error {
	switch arguments[0] {
	case "apply":
		return applyCommand(ctx, client, arguments[1:], stdin, stdout, stderr)
	case "get":
		return getCommand(ctx, client, arguments[1:], stdout)
	case "list":
		return listCommand(ctx, client, arguments[1:], stdout, stderr)
	case "delete":
		return deleteCommand(ctx, client, arguments[1:], stdout, stderr)
	case "doctor":
		return doctorCommand(ctx, client, config, stdout)
	case "help", "--help", "-h":
		printUsage(stdout)
		return nil
	default:
		return fmt.Errorf("unknown command %q", arguments[0])
	}
}

func applyCommand(ctx context.Context, client adminClient, arguments []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("apply", flag.ContinueOnError)
	flags.SetOutput(stderr)
	filename := flags.String("file", "-", "protobuf-JSON or YAML ApplyResourceRequest ('-' for stdin)")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return errors.New("apply accepts no positional arguments")
	}
	encoded, err := readInput(*filename, stdin)
	if err != nil {
		return err
	}
	request := &epochv1.ApplyResourceRequest{}
	if err := unmarshalYAMLProto(encoded, request); err != nil {
		return fmt.Errorf("decode apply request: %w", err)
	}
	if request.RequestToken == "" {
		request.RequestToken, err = requestToken()
		if err != nil {
			return err
		}
	}
	response, err := client.ApplyResource(ctx, request)
	if err != nil {
		return fmt.Errorf("apply resource: %w", err)
	}
	return writeProto(stdout, response)
}

func getCommand(ctx context.Context, client adminClient, arguments []string, stdout io.Writer) error {
	if len(arguments) != 1 {
		return errors.New("usage: epoch [global flags] get organization/project/environment/namespace/kind/name")
	}
	name, err := parseResourceName(arguments[0])
	if err != nil {
		return err
	}
	response, err := client.GetResource(ctx, &epochv1.GetResourceRequest{Name: name})
	if err != nil {
		return fmt.Errorf("get resource: %w", err)
	}
	return writeProto(stdout, response)
}

func listCommand(ctx context.Context, client adminClient, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	request := &epochv1.ListResourcesRequest{}
	var kind string
	var pageSize int
	flags.StringVar(&request.Organization, "organization", "", "organization scope")
	flags.StringVar(&request.Project, "project", "", "project scope")
	flags.StringVar(&request.Environment, "environment", "", "environment scope")
	flags.StringVar(&request.Namespace, "namespace", "", "namespace scope")
	flags.StringVar(&kind, "kind", "", "optional resource kind")
	flags.IntVar(&pageSize, "page-size", 100, "page size (1-1000)")
	flags.StringVar(&request.PageToken, "page-token", "", "resume token")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return errors.New("list accepts only flags")
	}
	if pageSize < 1 || pageSize > 1_000 {
		return errors.New("--page-size must be between 1 and 1000")
	}
	request.PageSize = int32(pageSize)
	if kind != "" {
		parsed, err := parseResourceKind(kind)
		if err != nil {
			return err
		}
		request.Kind = parsed
	}
	response, err := client.ListResources(ctx, request)
	if err != nil {
		return fmt.Errorf("list resources: %w", err)
	}
	return writeProto(stdout, response)
}

func deleteCommand(ctx context.Context, client adminClient, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("delete", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var expectedGeneration uint64
	flags.Uint64Var(&expectedGeneration, "expected-generation", 0, "optional optimistic-concurrency generation")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if len(flags.Args()) != 1 {
		return errors.New("usage: epoch [global flags] delete [--expected-generation N] organization/project/environment/namespace/kind/name")
	}
	name, err := parseResourceName(flags.Args()[0])
	if err != nil {
		return err
	}
	token, err := requestToken()
	if err != nil {
		return err
	}
	request := &epochv1.DeleteResourceRequest{RequestToken: token, Name: name}
	if expectedGeneration > 0 {
		request.ExpectedGeneration = &expectedGeneration
	}
	response, err := client.DeleteResource(ctx, request)
	if err != nil {
		return fmt.Errorf("delete resource: %w", err)
	}
	return writeProto(stdout, response)
}

func doctorCommand(ctx context.Context, client adminClient, config cliConfig, stdout io.Writer) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(config.httpEndpoint, "/")+"/healthz", nil)
	if err != nil {
		return err
	}
	response, err := (&http.Client{Timeout: config.timeout}).Do(request)
	if err != nil {
		return fmt.Errorf("control HTTP health: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("control HTTP health returned %s", response.Status)
	}
	if _, err := client.ListResources(ctx, &epochv1.ListResourcesRequest{PageSize: 1}); err != nil {
		return fmt.Errorf("authenticated RegionalAdmin probe: %w", err)
	}
	_, err = fmt.Fprintf(stdout, "ok\thttp=%s\tgrpc=%s\tauth=accepted\n", config.httpEndpoint, config.grpcEndpoint)
	return err
}

func parseResourceName(raw string) (*epochv1.ResourceName, error) {
	parts := strings.Split(strings.Trim(raw, "/"), "/")
	if len(parts) != 6 {
		return nil, errors.New("resource name must be organization/project/environment/namespace/kind/name")
	}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return nil, errors.New("resource name segments must not be empty")
		}
	}
	kind, err := parseResourceKind(parts[4])
	if err != nil {
		return nil, err
	}
	return &epochv1.ResourceName{Organization: parts[0], Project: parts[1], Environment: parts[2], Namespace: parts[3], Kind: kind, Name: parts[5]}, nil
}

func parseResourceKind(raw string) (epochv1.ResourceKind, error) {
	normalized := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(raw), "-", "_"))
	if normalized == "EVENTBUS" {
		normalized = "EVENT_BUS"
	}
	value, ok := epochv1.ResourceKind_value["RESOURCE_KIND_"+normalized]
	if !ok || value == int32(epochv1.ResourceKind_RESOURCE_KIND_UNSPECIFIED) {
		return epochv1.ResourceKind_RESOURCE_KIND_UNSPECIFIED, fmt.Errorf("unsupported resource kind %q", raw)
	}
	return epochv1.ResourceKind(value), nil
}

func unmarshalYAMLProto(encoded []byte, target proto.Message) error {
	jsonBytes, err := yaml.YAMLToJSONStrict(encoded)
	if err != nil {
		return err
	}
	return (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(jsonBytes, target)
}

func writeProto(writer io.Writer, message proto.Message) error {
	encoded, err := (protojson.MarshalOptions{Multiline: true, Indent: "  ", UseProtoNames: true}).Marshal(message)
	if err != nil {
		return err
	}
	if _, err := writer.Write(encoded); err != nil {
		return err
	}
	_, err = writer.Write([]byte("\n"))
	return err
}

func readInput(filename string, stdin io.Reader) ([]byte, error) {
	if filename == "-" {
		encoded, err := io.ReadAll(io.LimitReader(stdin, maxInputSize+1))
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		if len(encoded) > maxInputSize {
			return nil, errors.New("input exceeds 1 MiB")
		}
		return encoded, nil
	}
	clean := filepath.Clean(filename)
	encoded, err := os.ReadFile(clean)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", clean, err)
	}
	if len(encoded) > maxInputSize {
		return nil, errors.New("input exceeds 1 MiB")
	}
	return encoded, nil
}

func requestToken() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate request token: %w", err)
	}
	return "epoch-cli-" + hex.EncodeToString(random), nil
}

func bearerInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, response any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoke(ctx, method, request, response, connection, options...)
	}
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func printUsage(writer io.Writer) {
	fmt.Fprintln(writer, "Epoch management CLI")
	fmt.Fprintln(writer, "usage: epoch [--endpoint host:port] [--http-endpoint URL] [--token TOKEN] <command>")
	fmt.Fprintln(writer, "commands: apply, get, list, delete, doctor")
	fmt.Fprintln(writer, "resource name: organization/project/environment/namespace/kind/name")
}
