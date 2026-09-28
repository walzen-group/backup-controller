package restic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
)

// keys holds the master key of every repository the controller has opened,
// so each repository runs scrypt once for the life of the process. restic
// v0.18.1 calibrates scrypt to up to 60 MiB and 500 ms per key
// (internal/repository/key.go:51-55), so ten opens at once would need about
// 600 MiB.
var keys = &keyCache{masters: map[string]key{}}

// scryptSlot lets one scrypt run at a time in the process, so opening many
// repositories at once never holds more than one key derivation in memory.
var scryptSlot = make(chan struct{}, 1)

// keyCache maps a repository to its master key. The entry's name is a hash
// of the repository's location, the password and the names of its key
// files, so a changed password or a repository created again under the same
// location never finds an old entry.
type keyCache struct {
	mu      sync.Mutex
	masters map[string]key
}

// get returns the master key stored under an entry name, and whether there
// is one.
func (c *keyCache) get(entry string) (key, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	master, ok := c.masters[entry]
	return master, ok
}

// put stores a master key under an entry name.
func (c *keyCache) put(entry string, master key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.masters[entry] = master
}

// cacheEntry returns the name of a repository's entry in the key cache.
//
// Parameters:
//   - location identifies the repository, such as its S3 endpoint, bucket
//     and prefix.
//   - password is the repository password from the Secret.
//   - names are the names of the files under keys/.
//
// It returns a SHA-256 of the three, so the password is never kept as
// plain text in the cache.
func cacheEntry(location, password string, names []string) string {
	sum := sha256.Sum256([]byte(location + "\x00" + password + "\x00" + strings.Join(names, ",")))
	return hex.EncodeToString(sum[:])
}

// OpenCached opens a repository the way Open does, and derives each key at
// most once.
//
// Parameters:
//   - store holds the repository's files.
//   - location identifies the repository in the cache (see cacheEntry).
//   - password is the repository password.
//
// It lists keys/ on every call, which costs one S3 request. When the cache
// holds the master key for the same location, password and key files, it
// returns the repository at once. Otherwise it waits for the scrypt slot,
// looks in the cache again (another call may have opened the repository
// while it waited), opens the repository with Open's rules, and stores the
// master key. It returns ErrNoRepository when keys/ holds no files, the
// context's error when the context ends while it waits for the slot, and
// the other errors of Open.
func OpenCached(ctx context.Context, store Store, location, password string) (*Repository, error) {
	names, err := store.List(ctx, "keys")
	if err != nil {
		return nil, fmt.Errorf("list the key files: %w", err)
	}
	if len(names) == 0 {
		return nil, ErrNoRepository
	}
	entry := cacheEntry(location, password, names)
	if master, ok := keys.get(entry); ok {
		return &Repository{store: store, master: master}, nil
	}

	select {
	case scryptSlot <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-scryptSlot }()
	if master, ok := keys.get(entry); ok {
		return &Repository{store: store, master: master}, nil
	}
	master, err := openKeys(ctx, store, password, names)
	if err != nil {
		return nil, err
	}
	keys.put(entry, master)
	return &Repository{store: store, master: master}, nil
}

// openKeys tries each key file with the password and returns the master key
// of the first one the password opens.
//
// Parameters:
//   - store holds the key files under keys/.
//   - password is the repository password.
//   - names are the names of the key files, as listed.
//
// It returns an error when a key file can't be read or decoded, and when no
// key file opens with the password.
func openKeys(ctx context.Context, store Store, password string, names []string) (key, error) {
	for _, name := range names {
		raw, err := store.Get(ctx, path.Join("keys", name))
		if err != nil {
			return key{}, fmt.Errorf("read key file %s: %w", name, err)
		}
		master, err := masterKey(password, raw)
		if errors.Is(err, errWrongKey) {
			continue
		}
		if err != nil {
			return key{}, fmt.Errorf("key file %s: %w", name, err)
		}
		return master, nil
	}
	return key{}, errors.New("no key file opens with this password")
}
