package restic

import (
	"fmt"

	"golang.org/x/crypto/scrypt"
)

// kdf is the signature of scrypt.Key: it turns a password and a salt into
// keyLen bytes, with the cost parameters n, r and p.
type kdf func(password, salt []byte, n, r, p, keyLen int) ([]byte, error)

// keyDeriver runs the scrypt derivations that open key files, one at a time.
type keyDeriver struct {
	// kdf is the key derivation function, scrypt.Key in the controller.
	kdf kdf
	// gate holds a token while a derivation runs. Its capacity of one makes
	// every other derivation wait for the running one to end.
	gate chan struct{}
}

// newKeyDeriver returns a keyDeriver over a key derivation function that
// runs one derivation at a time.
//
// Parameters:
//   - derive is the key derivation function: scrypt.Key in the controller,
//     and a function that records how derivations overlap in the tests.
func newKeyDeriver(derive kdf) keyDeriver {
	return keyDeriver{kdf: derive, gate: make(chan struct{}, 1)}
}

// derivations is the keyDeriver every Open in the process goes through.
var derivations = newKeyDeriver(scrypt.Key)

// deriveKey runs scrypt over the password with the parameters from a key
// file, through derivations. See keyDeriver.derive.
func deriveKey(password string, file keyFile) (key, error) {
	return derivations.derive(password, file)
}

// derive runs scrypt over a password with the parameters from a key file,
// and splits the 64 bytes it produces into a key.
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
// each open repositories, so derive waits for the gate before it runs
// scrypt, and the process holds one derivation's memory at most.
func (d keyDeriver) derive(password string, file keyFile) (key, error) {
	if file.KDF != "scrypt" {
		return key{}, fmt.Errorf("key file uses kdf %q, and only scrypt is supported", file.KDF)
	}
	raw, err := d.run([]byte(password), file)
	if err != nil {
		return key{}, fmt.Errorf("derive the key: %w", err)
	}
	var k key
	copy(k.encrypt[:], raw[:32])
	copy(k.macK[:], raw[32:48])
	copy(k.macR[:], raw[48:64])
	return k, nil
}

// run runs the key derivation function over a password with the parameters
// from a key file, once no other derivation holds the gate, and returns the
// 64 bytes it derives or its error.
//
// It gives the gate back when the function returns, and when it panics too,
// so a panic that a reconciler recovers from leaves no later Open waiting.
func (d keyDeriver) run(password []byte, file keyFile) ([]byte, error) {
	d.gate <- struct{}{}
	defer func() { <-d.gate }()
	return d.kdf(password, file.Salt, file.N, file.R, file.P, 64)
}
