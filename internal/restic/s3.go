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

// ParseRepository reads a repository string in restic's s3 form, with the
// rule of ParseConfig in restic v0.18.1 (internal/backend/s3/config.go:60-106):
//
//	s3:http://host:10172/bucket/some/prefix
//	s3:https://host/bucket/some/prefix
//	s3://host/bucket/some/prefix
//	s3:host/bucket/some/prefix
//
// A string that starts with s3:http is a URL: its host is the endpoint, the
// first part of its path is the bucket and the rest is the prefix. Only the
// scheme http turns TLS off. Any other string drops s3:// or s3: and splits
// at the first two "/" into endpoint, bucket and prefix, over HTTPS. A
// prefix that is not empty is cleaned with path.Clean, so a trailing "/"
// has no effect.
//
// It returns an error for a string without the s3: prefix, for an s3:http
// URL that does not parse or has no path, and for a string that names no
// endpoint, as restic does. It also returns an error when the bucket is
// empty: restic reads such a string, but it cannot open a repository there.
func ParseRepository(repository string) (Location, error) {
	var endpoint, bucket, prefix string
	secure := true
	switch {
	case strings.HasPrefix(repository, "s3:http"):
		parsed, err := url.Parse(repository[len("s3:"):])
		if err != nil {
			return Location{}, fmt.Errorf("parse repository %q: %w", repository, err)
		}
		if parsed.Path == "" {
			return Location{}, fmt.Errorf("repository %q names no bucket", repository)
		}
		endpoint, secure = parsed.Host, parsed.Scheme != "http"
		bucket, prefix, _ = strings.Cut(parsed.Path[1:], "/")
	case strings.HasPrefix(repository, "s3://"):
		endpoint, bucket, prefix = splitRepository(repository[len("s3://"):])
	case strings.HasPrefix(repository, "s3:"):
		endpoint, bucket, prefix = splitRepository(repository[len("s3:"):])
	default:
		return Location{}, fmt.Errorf("repository %q is not an s3: repository", repository)
	}

	if endpoint == "" || bucket == "" {
		return Location{}, fmt.Errorf("repository %q names no endpoint or no bucket", repository)
	}
	if prefix != "" {
		prefix = path.Clean(prefix)
	}
	return Location{Endpoint: endpoint, Secure: secure, Bucket: bucket, Prefix: prefix}, nil
}

// splitRepository splits the part of an s3 repository string after s3: or
// s3:// at its first two "/" into endpoint, bucket and prefix, as restic
// v0.18.1 does (internal/backend/s3/config.go:84-88).
func splitRepository(s string) (endpoint, bucket, prefix string) {
	endpoint, rest, _ := strings.Cut(s, "/")
	bucket, prefix, _ = strings.Cut(rest, "/")
	return endpoint, bucket, prefix
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
// empty. It returns an error when the listing fails, or when ctx ends
// before the listing is complete.
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
	// minio-go ends the listing with no error when ctx is done between two
	// pages, so the names so far may not be all of them.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("list %s/%s: the listing stopped early: %w", s.at.Bucket, prefix, err)
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

// S3Lister lists and retimes the snapshots of a restic repository for the
// controller. Each call opens the repository that a VolSync repository Secret
// names, in its S3 bucket.
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
func (l S3Lister) Retime(ctx context.Context, secret *corev1.Secret, id string, at time.Time, tag string) (Snapshot, error) {
	repo, err := l.open(ctx, secret)
	if err != nil {
		return Snapshot{}, err
	}
	return repo.Retime(ctx, id, at, tag)
}
