package bootstrap

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestAMalformedEndpointCAIsAnError checks that endpointCA refuses an
// ObjectStore whose endpointCA name is not a string. A store with a malformed
// endpointCA is not a store with no endpointCA, so the webhook does not read
// the store with the public roots only.
func TestAMalformedEndpointCAIsAnError(t *testing.T) {
	store := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "store", "namespace": "app"},
		"spec": map[string]any{"configuration": map[string]any{
			"endpointCA": map[string]any{"name": int64(42), "key": "ca.crt"},
		}},
	}}
	bundle, err := endpointCA(context.Background(), nil, "app", store)
	if err == nil {
		t.Fatalf("endpointCA returned %q and no error for a malformed endpointCA name", bundle)
	}
}
