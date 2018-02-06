package intelligentstore

import (
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"io"
)

type Hash string

func NewHash(r io.Reader) (Hash, error) {
	hasher := newHasher()
	_, err := io.Copy(hasher, r)
	if nil != err {
		return "", err
	}

	return Hash(hex.EncodeToString(hasher.Sum(nil))), nil
}

func newHasher() hash.Hash {
	return sha512.New()
}

// FirstChunk is the first 2 tokens of the hash
func (h Hash) FirstChunk() string {
	return string(h)[0:2]
}

// Remainder is all the tokens of the hash, except the first 2 tokens
func (h Hash) Remainder() string {
	return string(h)[2:]
}
