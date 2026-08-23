// Package manifests validates rendered Kubernetes resources without requiring
// access to a live API server.
package manifests

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"epoch.local/epoch/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	maxManifestBytes   = 4 << 20
	maxManifestObjects = 100
)

// Validate strictly decodes a bounded multi-document Kubernetes render and
// returns the number of unique typed resources it contains.
func Validate(reader io.Reader) (int, error) {
	encoded, err := io.ReadAll(io.LimitReader(reader, maxManifestBytes+1))
	if err != nil {
		return 0, fmt.Errorf("read rendered manifests: %w", err)
	}
	if len(encoded) > maxManifestBytes {
		return 0, errors.New("rendered manifests exceeds 4 MiB")
	}

	scheme, err := validationScheme()
	if err != nil {
		return 0, err
	}
	decode := serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDeserializer()
	documents := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(encoded), 4*1024)
	identities := make(map[string]struct{})
	count := 0
	for {
		var raw json.RawMessage
		if err := documents.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("decode rendered YAML document %d: %w", count+1, err)
		}
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		if count == maxManifestObjects {
			return 0, fmt.Errorf("rendered manifests exceeds %d objects", maxManifestObjects)
		}
		object, groupVersionKind, err := decode.Decode(raw, nil, nil)
		if err != nil {
			return 0, fmt.Errorf("strictly decode rendered object %d: %w", count+1, err)
		}
		accessor, err := meta.Accessor(object)
		if err != nil {
			return 0, fmt.Errorf("read rendered object %d metadata: %w", count+1, err)
		}
		if accessor.GetName() == "" {
			return 0, fmt.Errorf("rendered object %d has no metadata.name", count+1)
		}
		identity := fmt.Sprintf(
			"%s/%s/%s",
			groupVersionKind.String(),
			accessor.GetNamespace(),
			accessor.GetName(),
		)
		if _, exists := identities[identity]; exists {
			return 0, fmt.Errorf("duplicate object %s", identity)
		}
		identities[identity] = struct{}{}
		count++
	}
	if count == 0 {
		return 0, errors.New("rendered manifests contains no objects")
	}
	return count, nil
}

func validationScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	registrations := []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		appsv1.AddToScheme,
		rbacv1.AddToScheme,
		apiextensionsv1.AddToScheme,
		v1alpha1.AddToScheme,
	}
	for _, register := range registrations {
		if err := register(scheme); err != nil {
			return nil, fmt.Errorf("register Kubernetes validation type: %w", err)
		}
	}
	return scheme, nil
}
