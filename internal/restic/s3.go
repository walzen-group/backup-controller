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

// The keys a VolSync restic repository Secret holds.
const (
	KeyRepository = "RESTIC_REPOSITORY"
	KeyPassword   = "RESTIC_PASSWORD"
	KeyAccessKey  = "AWS_ACCESS_KEY_ID"
	KeySecretKey  = "AWS_SECRET_ACCESS_KEY"
)

// Location is a repository in an S3 bucket and the keys to open it.
type Location struct {
	Endpoint  string
	Secure    bool
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
	Password  string
}

// FromSecret reads a Location out of a VolSync repository Secret.
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

// ParseRepository reads restic's s3 repository form. An explicit scheme
// chooses TLS, and a bare host means HTTPS, which is restic's own default:
//
//	s3:http://host:10172/bucket/some/prefix
//	s3:host/bucket/some/prefix
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

// S3Store keeps a repository's files in its bucket.
type S3Store struct {
	client *minio.Client
	at     Location
}

// NewS3Store connects to the repository's endpoint.
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

func (s *S3Store) key(name string) string {
	return strings.TrimPrefix(path.Join(s.at.Prefix, name), "/")
}

// List returns the file names directly under dir.
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

// Lister returns a repository's snapshots. The interface lets the controllers
// run their tests without an object store.
type Lister interface {
	Snapshots(ctx context.Context, secret *corev1.Secret) ([]Snapshot, error)
}

// Retimer moves a snapshot to another time and tags it. The interface lets the
// controllers run their tests without an object store.
type Retimer interface {
	Retime(ctx context.Context, secret *corev1.Secret, short string, at time.Time, tag string) (Snapshot, error)
}

// S3Lister opens the repository a VolSync Secret names, to list it or to
// rewrite one of its snapshots.
type S3Lister struct{}

// open connects to the repository a VolSync Secret names.
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

// Snapshots opens the repository and returns its snapshots, oldest first. A
// repository VolSync has not initialised yet holds no snapshots.
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

// Retime opens the repository and moves one snapshot, see Repository.Retime.
func (l S3Lister) Retime(ctx context.Context, secret *corev1.Secret, short string, at time.Time, tag string) (Snapshot, error) {
	repo, err := l.open(ctx, secret)
	if err != nil {
		return Snapshot{}, err
	}
	return repo.Retime(ctx, short, at, tag)
}
