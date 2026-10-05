package epoch

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/protobuf/proto"
)

func TestManagementSharedWireVectorsPreserveUnsignedGenerationPresence(t *testing.T) {
	data, err := os.ReadFile("../../../spec/fixtures/sdk-management-wire-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Schema  string                `json:"schema"`
		Token   string                `json:"request_token"`
		Name    *epochv1.ResourceName `json:"name"`
		Vectors []struct {
			Generation *uint64 `json:"expected_generation"`
			Present    bool    `json:"expected_presence"`
			Hex        string  `json:"protobuf_hex"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Schema != "epoch.sdk.management-wire/v1" || len(fixture.Vectors) != 3 {
		t.Fatal("review changed cross-language management fixture")
	}
	for _, vector := range fixture.Vectors {
		request := &epochv1.DeleteResourceRequest{RequestToken: fixture.Token, Name: fixture.Name, ExpectedGeneration: vector.Generation}
		encoded, err := proto.Marshal(request)
		if err != nil || hex.EncodeToString(encoded) != vector.Hex {
			t.Fatalf("cross-language delete wire bytes changed: %x, %v", encoded, err)
		}
		var decoded epochv1.DeleteResourceRequest
		if err := proto.Unmarshal(encoded, &decoded); err != nil || !proto.Equal(request, &decoded) || (decoded.ExpectedGeneration != nil) != vector.Present {
			t.Fatal("cross-language uint64 or optional presence changed", err)
		}
	}
}
