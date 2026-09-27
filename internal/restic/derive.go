package restic

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"golang.org/x/crypto/scrypt"
)

// kdf is the signature of scrypt.Key: it turns a password and a salt into
// keyLen bytes, with the cost parameters n, r and p.
type kdf func(password, salt []byte, n, r, p, keyLen int) ([]byte, error)

// maxDerivedKeys bounds how many derived keys a keyDeriver keeps. There is
// one for each repository key file the controller opens, so a cluster
// reaches it only with more repositories than that; the keyDeriver then
// forgets them all and derives each again once.
const maxDerivedKeys = 256

// derivation names one scrypt derivation: the SHA-256 of the password and
// of the key file's salt and cost parameters, the whole input scrypt reads.
type derivation [sha256.Size]byte

// keyDeriver runs the scrypt derivations that open key files, one at a time,
// and keeps each key it derives.
type keyDeriver struct {
	// kdf is the key derivation function, scrypt.Key in the controller.
	kdf kdf
	// gate holds a token while a derivation runs. Its capacity of one makes
	// every other derivation wait for the running one to end, and it guards
	// derived.
	gate chan struct{}
	// derived holds the key of each derivation that has run, read and
	// written only while holding gate.
	derived map[derivation]key
}

// newKeyDeriver returns a keyDeriver over a key derivation function that
// runs one derivation at a time and keeps what it derives.
//
// Parameters:
//   - derive is the key derivation function: scrypt.Key in the controller,
//     and a function that records its calls in the tests.
func newKeyDeriver(derive kdf) *keyDeriver {
	return &keyDeriver{kdf: derive, gate: make(chan struct{}, 1), derived: map[derivation]key{}}
}

// derivations is the keyDeriver every Open in the process goes through.
var derivations = newKeyDeriver(scrypt.Key)

// deriveKey runs scrypt over the password with the parameters from a key
// file, through derivations. See keyDeriver.derive.
func deriveKey(password string, file keyFile) (key, error) {
	return derivations.derive(password, file)
}

// derive returns the key that scrypt derives from a password with the
// parameters from a key file.
//
// Parameters:
//   - password is the repository password from the repository Secret.
//   - file is the decoded key file, which carries the scrypt parameters and
//     the salt.
//
// It returns an error when the key file names a KDF other than scrypt, or
// when scrypt refuses the parameters.
//
// scrypt holds 128*N*r bytes while it runs, 32 MiB with restic's parameters
// (N=32768, r=8). The BackupRun and RestoreRun reconcilers and the populator
// each open repositories, often the same ones again, so derive runs one
// derivation at a time, and returns the kept key for a password and key file
// it has derived before. scrypt is deterministic, so the kept key is the one
// a new derivation would give.
func (d *keyDeriver) derive(password string, file keyFile) (key, error) {
	if file.KDF != "scrypt" {
		return key{}, fmt.Errorf("key file uses kdf %q, and only scrypt is supported", file.KDF)
	}
	d.gate <- struct{}{}
	defer func() { <-d.gate }()

	id := derivationOf(password, file)
	if k, ok := d.derived[id]; ok {
		return k, nil
	}
	raw, err := d.kdf([]byte(password), file.Salt, file.N, file.R, file.P, 64)
	if err != nil {
		return key{}, fmt.Errorf("derive the key: %w", err)
	}
	var k key
	copy(k.encrypt[:], raw[:32])
	copy(k.macK[:], raw[32:48])
	copy(k.macR[:], raw[48:64])
	if len(d.derived) >= maxDerivedKeys {
		clear(d.derived)
	}
	d.derived[id] = k
	return k, nil
}

// derivationOf returns the derivation that scrypt runs for a password and a
// key file. Each variable-length part is prefixed with its length, so no two
// inputs hash the same bytes.
func derivationOf(password string, file keyFile) derivation {
	input := binary.BigEndian.AppendUint64(nil, uint64(len(password)))
	input = append(input, password...)
	input = binary.BigEndian.AppendUint64(input, uint64(len(file.Salt)))
	input = append(input, file.Salt...)
	for _, cost := range []int{file.N, file.R, file.P} {
		input = binary.BigEndian.AppendUint64(input, uint64(cost))
	}
	return sha256.Sum256(input)
}
