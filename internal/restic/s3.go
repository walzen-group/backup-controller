package restic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	corev1 "k8s.io/api/core/v1"
)

// The keys that a VolSync restic repository Secret holds and FromSecret reads.
const (
	KeyRepository = "RESTIC_REPOSITORY"
	KeyPassword   = "RESTIC_PASSWORD"
	KeyAccessKey  = "AWS_ACCESS_KEY_ID"
	KeySecretKey  = "AWS_SECRET_ACCESS_KEY"
)

// Location is a repository in an S3 bucket, with the credentials and the
// password that open it. FromSecret builds one from a VolSync repository
// Secret.
type Location struct {
	// Endpoint is the S3 server's host, with its port when the repository
	// string names one.
	Endpoint string
	// Secure is true when the client talks HTTPS to the endpoint.
	Secure bool
	// Bucket is the bucket that holds the repository.
	Bucket string
	// Prefix is the path of the repository inside the bucket. It's empty for a
	// repository at the bucket's root.
	Prefix string
	// AccessKey and SecretKey are the S3 credentials.
	AccessKey string
	SecretKey string
	// Password is the restic repository password.
	Password string
}

// FromSecret reads a Location out of a VolSync repository Secret. It parses
// RESTIC_REPOSITORY with ParseRepository, and takes the password and the S3
// credentials from the other three keys. When a key is missing or empty, it
// returns an error that names the Secret and the key.
func FromSecret(secret *corev1.Secret) (Location, error) {
	value := func(key string) (string, error) {
		raw, ok := secret.Data[key]
		if !ok || len(raw) == 0 {
			return "", fmt.Errorf("secret %s/%s has no %s", secret.Namespace, secret.Name, key)
		}
		return string(raw), nil
	}

	repository, err := value(KeyRepository)
	if err != nil {
		return Location{}, err
	}
	at, err := ParseRepository(repository)
	if err != nil {
		return Location{}, err
	}
	if at.Password, err = value(KeyPassword); err != nil {
		return Location{}, err
	}
	if at.AccessKey, err = value(KeyAccessKey); err != nil {
		return Location{}, err
	}
	if at.SecretKey, err = value(KeySecretKey); err != nil {
		return Location{}, err
	}
	return at, nil
}

// ParseRepository reads a repository string in restic's s3 form. When the
// string has a scheme, http turns TLS off and https keeps it on. When it has
// no scheme, the client uses HTTPS, which is also restic's default:
//
//	s3:http://host:10172/bucket/some/prefix
//	s3:host/bucket/some/prefix
//
// It returns an error for a string without the s3: prefix, for a scheme other
// than http or https, and for a string that names no endpoint or no bucket.
func ParseRepository(repository string) (Location, error) {
	rest, ok := strings.CutPrefix(repository, "s3:")
	if !ok {
		return Location{}, fmt.Errorf("repository %q is not an s3: repository", repository)
	}

	at := Location{Secure: true}
	if strings.Contains(rest, "://") {
		parsed, err := url.Parse(rest)
		if err != nil {
			return Location{}, fmt.Errorf("parse repository %q: %w", repository, err)
		}
		switch parsed.Scheme {
		case "http":
			at.Secure = false
		case "https":
		default:
			return Location{}, fmt.Errorf("repository %q has scheme %q", repository, parsed.Scheme)
		}
		at.Endpoint = parsed.Host
		rest = parsed.Path
	} else {
		host, remainder, _ := strings.Cut(rest, "/")
		at.Endpoint = host
		rest = remainder
	}

	rest = strings.Trim(rest, "/")
	bucket, prefix, _ := strings.Cut(rest, "/")
	if at.Endpoint == "" || bucket == "" {
		return Location{}, fmt.Errorf("repository %q names no endpoint or no bucket", repository)
	}
	at.Bucket = bucket
	at.Prefix = prefix
	return at, nil
}

// S3Store keeps a repository's files in the bucket that a Location names,
// under the Location's prefix.
type S3Store struct {
	client *minio.Client
	at     Location
}

// NewS3Store builds a MinIO client for the Location's endpoint, with its
// access key and secret key.
func NewS3Store(at Location) (*S3Store, error) {
	client, err := minio.New(at.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(at.AccessKey, at.SecretKey, ""),
		Secure: at.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf("build the object store client: %w", err)
	}
	return &S3Store{client: client, at: at}, nil
}

// key returns the object key of a repository file: its name under the
// Location's prefix, with no leading slash.
func (s *S3Store) key(name string) string {
	return strings.TrimPrefix(path.Join(s.at.Prefix, name), "/")
}

// List returns the names of the files directly under dir, and leaves out
// anything in a deeper directory. When dir doesn't exist, the listing is
// empty.
func (s *S3Store) List(ctx context.Context, dir string) ([]string, error) {
	prefix := s.key(dir) + "/"
	var names []string
	for object := range s.client.ListObjects(ctx, s.at.Bucket, minio.ListObjectsOptions{Prefix: prefix}) {
		if object.Err != nil {
			return nil, fmt.Errorf("list %s/%s: %w", s.at.Bucket, prefix, object.Err)
		}
		name := strings.TrimPrefix(object.Key, prefix)
		if name != "" && !strings.Contains(name, "/") {
			names = append(names, name)
		}
	}
	return names, nil
}

// Get reads one file.
func (s *S3Store) Get(ctx context.Context, name string) ([]byte, error) {
	object, err := s.client.GetObject(ctx, s.at.Bucket, s.key(name), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s/%s: %w", s.at.Bucket, s.key(name), err)
	}
	defer func() { _ = object.Close() }()
	raw, err := io.ReadAll(object)
	if err != nil {
		return nil, fmt.Errorf("read %s/%s: %w", s.at.Bucket, s.key(name), err)
	}
	return raw, nil
}

// Put writes one file.
func (s *S3Store) Put(ctx context.Context, name string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.at.Bucket, s.key(name), bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return fmt.Errorf("put %s/%s: %w", s.at.Bucket, s.key(name), err)
	}
	return nil
}

// Remove deletes one file.
func (s *S3Store) Remove(ctx context.Context, name string) error {
	if err := s.client.RemoveObject(ctx, s.at.Bucket, s.key(name), minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("remove %s/%s: %w", s.at.Bucket, s.key(name), err)
	}
	return nil
}

// Lister lists the snapshots in the repository that a VolSync repository Secret
// names. S3Lister is the implementation the controller runs with. The
// controllers take the interface so that their tests can run without an
// object store.
type Lister interface {
	Snapshots(ctx context.Context, secret *corev1.Secret) ([]Snapshot, error)
}

// Retimer changes the time of one snapshot and adds a tag to it, in the
// repository that a VolSync repository Secret names. See Repository.Retime.
// S3Lister is the implementation the controller runs with. The controllers
// take the interface so that their tests can run without an object store.
type Retimer interface {
	Retime(ctx context.Context, secret *corev1.Secret, short string, at time.Time, tag string) (Snapshot, error)
}

// S3Lister is the Lister and Retimer the controller runs with. Each call opens
// the repository that a VolSync repository Secret names, in its S3 bucket.
type S3Lister struct{}

// open reads the Location from a VolSync repository Secret, connects to its
// bucket, and opens the repository with the Secret's password.
func (S3Lister) open(ctx context.Context, secret *corev1.Secret) (*Repository, error) {
	at, err := FromSecret(secret)
	if err != nil {
		return nil, err
	}
	store, err := NewS3Store(at)
	if err != nil {
		return nil, err
	}
	return Open(ctx, store, at.Password)
}

// Snapshots opens the repository that the Secret names and returns its
// snapshots, oldest first. A repository that VolSync hasn't initialised yet
// counts as holding no snapshots, so Snapshots returns an empty list and no
// error for it.
func (l S3Lister) Snapshots(ctx context.Context, secret *corev1.Secret) ([]Snapshot, error) {
	repo, err := l.open(ctx, secret)
	if errors.Is(err, ErrNoRepository) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return repo.Snapshots(ctx)
}

// Retime opens the repository that the Secret names and calls
// Repository.Retime on it with the other arguments. Repository.Retime
// describes what they mean.
func (l S3Lister) Retime(ctx context.Context, secret *corev1.Secret, short string, at time.Time, tag string) (Snapshot, error) {
	repo, err := l.open(ctx, secret)
	if err != nil {
		return Snapshot{}, err
	}
	return repo.Retime(ctx, short, at, tag)
}
