package volsync

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestSecretCopyPreservesDataAndType checks that the copy from SecretCopy has
// the claim UID as its name and keeps the type and data. It also checks that the
// data bytes are copied, so a change to the copy leaves the original alone.
func TestSecretCopyPreservesDataAndType(t *testing.T) {
	repo := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-secret", Namespace: "apps"},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"repository": []byte("s3://bucket"), "password": []byte("secret")},
	}
	copy := SecretCopy(repo, types.UID("claim-123"), "backup-system")
	if copy.Name != "claim-123" || copy.Namespace != "backup-system" {
		t.Fatalf("secret identity = %s/%s, want backup-system/claim-123", copy.Namespace, copy.Name)
	}
	if copy.Type != repo.Type || !reflect.DeepEqual(copy.Data, repo.Data) {
		t.Fatalf("secret copy = %#v, want type and data from repository", copy)
	}
	copy.Data["password"][0] = 'X'
	if string(repo.Data["password"]) != "secret" {
		t.Fatal("SecretCopy aliases repository data")
	}
}
