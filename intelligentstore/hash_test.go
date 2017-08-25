package intelligentstore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

// from echo "abcde123" | sha512sum
// bdd8c0343b958f9f6b6d8e413beeebedd34c977bd43612bbecb52bc101aadc833758da00610c8ef2b67ec51cf9ccd021708bb0b327ebd63ee693411f5d4c9699
func Test_NewHash(t *testing.T) {
	expected := "bdd8c0343b958f9f6b6d8e413beeebedd34c977bd43612bbecb52bc101aadc833758da00610c8ef2b67ec51cf9ccd021708bb0b327ebd63ee693411f5d4c9699"

	hash, err := NewHash(bytes.NewBuffer([]byte("abcde123")))
	assert.Nil(t, err)
	assert.Equal(t, expected, string(hash))
}

func Test_FirstChunk(t *testing.T) {
	hash, err := NewHash(bytes.NewBuffer([]byte("abcde123")))
	assert.Nil(t, err)
	assert.Equal(t, "bd", hash.FirstChunk())
}

func Test_Remainder(t *testing.T) {
	hash, err := NewHash(bytes.NewBuffer([]byte("abcde123")))
	assert.Nil(t, err)
	assert.Equal(t, "bd", hash.Remainder())
}
