package bootstrap

import "testing"

// TestSplitEndpoint checks the host and TLS flag splitEndpoint returns for
// each form an ObjectStore's endpointURL takes, bare hosts with a port among
// them.
func TestSplitEndpoint(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		host     string
		secure   bool
	}{
		{"https://s3.example.com", "s3.example.com", true},
		{"https://s3.example.com:9000", "s3.example.com:9000", true},
		{"http://minio.minio.svc:9000", "minio.minio.svc:9000", false},
		{"s3.example.com", "s3.example.com", true},
		{"s3.example.com:9000", "s3.example.com:9000", true},
		{"10.0.0.1:9000", "10.0.0.1:9000", true},
	} {
		host, secure, err := splitEndpoint(tc.endpoint)
		if err != nil {
			t.Errorf("splitEndpoint(%q): %v", tc.endpoint, err)
			continue
		}
		if host != tc.host || secure != tc.secure {
			t.Errorf("splitEndpoint(%q) = %q, %v, want %q, %v", tc.endpoint, host, secure, tc.host, tc.secure)
		}
	}
	for _, endpoint := range []string{"", "ftp://s3.example.com"} {
		if _, _, err := splitEndpoint(endpoint); err == nil {
			t.Errorf("splitEndpoint(%q) succeeded, want an error", endpoint)
		}
	}
}
