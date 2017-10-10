package domain

import (
	"crypto/sha512"
	"encoding/hex"
	"io"
)

type Hash string

func NewHash(r io.Reader) (Hash, error) {
	hasher := sha512.New()
	_, err := io.Copy(hasher, r)
	if nil != err {
		return "", err
	}

	return Hash(hex.EncodeToString(hasher.Sum(nil))), nil
}

// FirstChunk is the first 2 tokens of the hash
func (h Hash) FirstChunk() string {
	return string(h)[0:2]
}

// Remainder is all the tokens of the hash, except the first 2 tokens
func (h Hash) Remainder() string {
	return string(h)[2:]
}
