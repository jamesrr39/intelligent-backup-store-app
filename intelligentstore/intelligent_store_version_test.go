package intelligentstore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_areFilesTheSame(t *testing.T) {
	bytesA := []byte("abc")
	fileB := bytes.NewBuffer([]byte("abc"))

	result, err := areFilesTheSameBytes(bytesA, fileB)
	assert.Nil(t, err)
	assert.True(t, result)

	fileC := bytes.NewBuffer([]byte("abd"))

	result, err = areFilesTheSameBytes(bytesA, fileC)
	assert.Nil(t, err)
	assert.False(t, result)

	fileD := bytes.NewBuffer([]byte("abcd"))

	result, err = areFilesTheSameBytes(bytesA, fileD)
	assert.Nil(t, err)
	assert.False(t, result)
}
