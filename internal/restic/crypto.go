package restic

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
	//nolint:staticcheck // restic's format is Poly1305-AES; this verifies its MACs and encrypts nothing new.
	"golang.org/x/crypto/poly1305"
	"golang.org/x/crypto/scrypt"
)

// The layout restic's design document gives every encrypted file:
// IV || CIPHERTEXT || MAC, with a 16-byte IV and a 16-byte Poly1305-AES MAC.
const (
	ivSize  = 16
	macSize = 16
)

// errWrongKey is what a MAC mismatch means in practice: the key tried was not
// this repository's, because the password was wrong or the key file belongs to
// another repository.
var errWrongKey = errors.New("MAC does not verify: wrong password or wrong key")

// key is one encryption key and its MAC key, as restic splits them.
//
// A key file's key comes out of scrypt as 64 bytes, a 32-byte AES-256 key then
// the 16-byte AES key k and the 16-byte Poly1305 key r. The master key it
// unlocks carries the same three parts as base64 in JSON.
type key struct {
	encrypt [32]byte
	macK    [16]byte
	macR    [16]byte
}

// masterKeyJSON is the document a key file's data decrypts to.
type masterKeyJSON struct {
	MAC struct {
		K []byte `json:"k"`
		R []byte `json:"r"`
	} `json:"mac"`
	Encrypt []byte `json:"encrypt"`
}

// keyFile is one document under keys/.
type keyFile struct {
	KDF  string `json:"kdf"`
	N    int    `json:"N"`
	R    int    `json:"r"`
	P    int    `json:"p"`
	Salt []byte `json:"salt"`
	Data []byte `json:"data"`
}

// deriveKey runs scrypt over the password with the key file's parameters.
func deriveKey(password string, file keyFile) (key, error) {
	if file.KDF != "scrypt" {
		return key{}, fmt.Errorf("key file uses kdf %q, and only scrypt is supported", file.KDF)
	}
	raw, err := scrypt.Key([]byte(password), file.Salt, file.N, file.R, file.P, 64)
	if err != nil {
		return key{}, fmt.Errorf("derive the key: %w", err)
	}
	var k key
	copy(k.encrypt[:], raw[:32])
	copy(k.macK[:], raw[32:48])
	copy(k.macR[:], raw[48:64])
	return k, nil
}

// masterKey decrypts a key file's data with the password-derived key.
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

// decrypt checks the MAC over the ciphertext and then decrypts it.
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

// mac computes Poly1305-AES: the one-time key is r followed by AES_k(IV).
// poly1305 clamps r itself, which is the masking restic applies to it.
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

// unpack strips repository version 2's encoding header from a decrypted
// unpacked file. A plaintext opening with '{' or '[' is JSON as it stands; one
// opening with the byte 2 is zstd-compressed JSON.
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
