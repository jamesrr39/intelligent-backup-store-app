package intelligentstore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// from echo "abcde123" | sha512sum
// bdd8c0343b958f9f6b6d8e413beeebedd34c977bd43612bbecb52bc101aadc833758da00610c8ef2b67ec51cf9ccd021708bb0b327ebd63ee693411f5d4c9699
func Test_NewHash(t *testing.T) {
	expected := "c4fe33bada9d6f5f5b04c0ef9b7e784fbb95cf5dea7718ab9eb964330ba0de85bac0076326f178a004b969490a50e11c04e9cbce327974e64a60d7eaae7902ba"

	hash, err := NewHash(bytes.NewBuffer([]byte("abcde123")))
	assert.Nil(t, err)
	assert.Equal(t, expected, string(hash))
}

func Test_FirstChunk(t *testing.T) {
	hash, err := NewHash(bytes.NewBuffer([]byte("abcde123")))
	assert.Nil(t, err)
	assert.Equal(t, "c4", hash.FirstChunk())
}

func Test_Remainder(t *testing.T) {
	expected := "fe33bada9d6f5f5b04c0ef9b7e784fbb95cf5dea7718ab9eb964330ba0de85bac0076326f178a004b969490a50e11c04e9cbce327974e64a60d7eaae7902ba"

	hash, err := NewHash(bytes.NewBuffer([]byte("abcde123")))
	assert.Nil(t, err)
	assert.Equal(t, expected, hash.Remainder())
}

func Test_NewHash_bad_writer(t *testing.T) {
	_, err := NewHash(&badWriter{})
	assert.NotNil(t, err)
}

type badWriter struct{}

func (w *badWriter) Read(b []byte) (int, error) {
	return 0, errors.New("bad reader")
}
