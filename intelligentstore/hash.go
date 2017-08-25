package intelligentstore

import (
	"crypto/sha512"
	"encoding/hex"
	"io"
	"os"
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

func NewHashFromFilePath(filePath string) (Hash, error) {
	file, err := os.Open(filePath)
	if nil != err {
		return "", err
	}
	defer file.Close()

	return NewHash(file)
}

func (h Hash) FirstChunk() string {
	return string(h)[0:2]
}

func (h Hash) Remainder() string {
	return string(h)[2:]
}
