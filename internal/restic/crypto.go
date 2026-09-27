package restic

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	//nolint:staticcheck // restic's format is Poly1305-AES, and a file this package writes has to carry that MAC.
	"golang.org/x/crypto/poly1305"
	"golang.org/x/crypto/scrypt"
)

// ivSize and macSize are the sizes of the parts of an encrypted file. restic's
// design document lays out every encrypted file as IV || CIPHERTEXT || MAC,
// with a 16-byte IV and a 16-byte Poly1305-AES MAC.
const (
	ivSize  = 16
	macSize = 16
)

// errWrongKey is the error for a MAC that doesn't verify. In practice it means
// the key isn't this repository's: the password was wrong, or the key file
// belongs to another repository. Open skips a key file that fails this way and
// tries the next one.
var errWrongKey = errors.New("MAC does not verify: wrong password or wrong key")

// key is one restic key, split into three parts the way restic splits it: an
// AES-256 key that encrypts the data, and the two parts of the Poly1305-AES MAC
// key.
//
// For a key file, scrypt turns the password into 64 bytes. The first 32 are
// the AES-256 key, the next 16 are the MAC's AES key k, and the last 16 are the
// Poly1305 key r. The master key that a key file unlocks holds the same three
// parts, as base64 in JSON.
type key struct {
	encrypt [32]byte
	macK    [16]byte
	macR    [16]byte
}

// masterKeyJSON is the JSON document that a key file's data decrypts to. It
// holds the repository's master key, with each part in base64, which
// encoding/json decodes into the byte slices.
type masterKeyJSON struct {
	MAC struct {
		K []byte `json:"k"`
		R []byte `json:"r"`
	} `json:"mac"`
	Encrypt []byte `json:"encrypt"`
}

// keyFile is the JSON document in one file under keys/. It holds the scrypt
// parameters (N, r, p and the salt) and, in Data, the master key encrypted with
// the key that those parameters derive from the password.
type keyFile struct {
	KDF  string `json:"kdf"`
	N    int    `json:"N"`
	R    int    `json:"r"`
	P    int    `json:"p"`
	Salt []byte `json:"salt"`
	Data []byte `json:"data"`
}

// masterKey opens one key file with a password and returns the master key
// inside it. The raw argument is the key file's content as stored. masterKey
// derives a key from the password, decrypts the file's data with it, and
// decodes the master key from the result. It returns errWrongKey when the
// password doesn't open this key file.
func masterKey(password string, raw []byte) (key, error) {
	var file keyFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return key{}, fmt.Errorf("decode the key file: %w", err)
	}
	derived, err := deriveKey(password, file)
	if err != nil {
		return key{}, err
	}
	plain, err := derived.decrypt(file.Data)
	if err != nil {
		return key{}, err
	}

	var doc masterKeyJSON
	if err := json.Unmarshal(plain, &doc); err != nil {
		return key{}, fmt.Errorf("decode the master key: %w", err)
	}
	if len(doc.Encrypt) != 32 || len(doc.MAC.K) != 16 || len(doc.MAC.R) != 16 {
		return key{}, fmt.Errorf("the master key has the wrong length")
	}
	var master key
	copy(master.encrypt[:], doc.Encrypt)
	copy(master.macK[:], doc.MAC.K)
	copy(master.macR[:], doc.MAC.R)
	return master, nil
}

// decrypt opens one encrypted file, laid out as IV || CIPHERTEXT || MAC. It
// checks the MAC over the ciphertext first and returns errWrongKey when it
// doesn't match. Then it decrypts the ciphertext with AES-256 in CTR mode.
func (k key) decrypt(sealed []byte) ([]byte, error) {
	if len(sealed) < ivSize+macSize {
		return nil, fmt.Errorf("an encrypted file of %d bytes is shorter than its overhead", len(sealed))
	}
	iv := sealed[:ivSize]
	ciphertext := sealed[ivSize : len(sealed)-macSize]
	mac := sealed[len(sealed)-macSize:]

	want, err := k.mac(iv, ciphertext)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare(want[:], mac) != 1 {
		return nil, errWrongKey
	}

	block, err := aes.NewCipher(k.encrypt[:])
	if err != nil {
		return nil, fmt.Errorf("build the AES cipher: %w", err)
	}
	plain := make([]byte, len(ciphertext))
	cipher.NewCTR(block, iv).XORKeyStream(plain, ciphertext)
	return plain, nil
}

// seal encrypts a plaintext with AES-256 in CTR mode under a new random IV, and
// returns IV || CIPHERTEXT || MAC, the layout decrypt reads. The MAC covers the
// ciphertext.
func (k key) seal(plain []byte) ([]byte, error) {
	iv := make([]byte, ivSize)
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("draw an IV: %w", err)
	}
	block, err := aes.NewCipher(k.encrypt[:])
	if err != nil {
		return nil, fmt.Errorf("build the AES cipher: %w", err)
	}
	ciphertext := make([]byte, len(plain))
	cipher.NewCTR(block, iv).XORKeyStream(ciphertext, plain)
	mac, err := k.mac(iv, ciphertext)
	if err != nil {
		return nil, err
	}

	sealed := make([]byte, 0, ivSize+len(ciphertext)+macSize)
	sealed = append(sealed, iv...)
	sealed = append(sealed, ciphertext...)
	return append(sealed, mac[:]...), nil
}

// mac computes the Poly1305-AES MAC of a message under the given IV. The
// one-time Poly1305 key is r followed by AES_k(IV), where k and r are the key's
// two MAC parts. The poly1305 package clamps r itself, which is the same
// masking restic applies to it.
func (k key) mac(iv, message []byte) ([16]byte, error) {
	block, err := aes.NewCipher(k.macK[:])
	if err != nil {
		return [16]byte{}, fmt.Errorf("build the MAC cipher: %w", err)
	}
	var oneTime [32]byte
	copy(oneTime[:16], k.macR[:])
	block.Encrypt(oneTime[16:], iv)

	var out [16]byte
	poly1305.Sum(&out, message, &oneTime)
	return out, nil
}

// unpack removes the encoding header that repository version 2 puts on a
// decrypted file stored outside the pack files, such as a snapshot or a lock.
// A plaintext that starts with '{' or '[' is uncompressed JSON, and unpack
// returns it as it is. A plaintext that starts with the byte 2 is
// zstd-compressed JSON, and unpack decompresses it. Any other first byte is an
// error.
func unpack(plain []byte) ([]byte, error) {
	if len(plain) == 0 {
		return nil, fmt.Errorf("the file is empty")
	}
	switch plain[0] {
	case '{', '[':
		return plain, nil
	case 2:
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("build the zstd decoder: %w", err)
		}
		defer decoder.Close()
		out, err := decoder.DecodeAll(plain[1:], nil)
		if err != nil {
			return nil, fmt.Errorf("decompress: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown encoding version %d", plain[0])
	}
}

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
	// every other derivation wait for the running one to end.
	gate chan struct{}
	// mu guards derived.
	mu sync.Mutex
	// derived holds the key of each derivation that has run, read and
	// written only while holding mu.
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
//
// restic sets p when it writes a key file: it raises p until one derivation
// takes about 0.5s on the machine that runs `restic init`. The recorded key
// files have p=8 and p=9, so one derivation takes 0.5s of CPU or more. Three
// Opens of cold repositories at once wait in the gate one after another.
// derive looks for a kept key before it takes the gate. Thus the Open of a
// kept key does not wait for a derivation of another key file. After it takes
// the gate, derive looks again, because the derivation before it can have
// kept the same key.
func (d *keyDeriver) derive(password string, file keyFile) (key, error) {
	if file.KDF != "scrypt" {
		return key{}, fmt.Errorf("key file uses kdf %q, and only scrypt is supported", file.KDF)
	}
	id := derivationOf(password, file)
	if k, ok := d.kept(id); ok {
		return k, nil
	}
	d.gate <- struct{}{}
	defer func() { <-d.gate }()
	if k, ok := d.kept(id); ok {
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
	d.keep(id, k)
	return k, nil
}

// kept returns the key that the keyDeriver keeps for a derivation.
//
// Parameters:
//   - id is the derivation, from derivationOf.
//
// It returns ok false when the keyDeriver keeps no key for id.
func (d *keyDeriver) kept(id derivation) (k key, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	k, ok = d.derived[id]
	return k, ok
}

// keep records the key of a derivation. When the keyDeriver already keeps
// maxDerivedKeys keys, keep removes all of them first.
//
// Parameters:
//   - id is the derivation, from derivationOf.
//   - k is the key that the derivation gave.
func (d *keyDeriver) keep(id derivation, k key) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.derived) >= maxDerivedKeys {
		clear(d.derived)
	}
	d.derived[id] = k
}

// derivationOf returns the derivation that scrypt runs for a password and a
// key file.
//
// Parameters:
//   - password is the repository password.
//   - file is the decoded key file. Its salt, N, r and p go into the hash,
//     so a key file with other cost parameters gets its own derivation.
//
// It returns the SHA-256 of the password, the salt and the cost parameters.
// Each part of variable length has its length before it, so no two inputs
// hash the same bytes.
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
