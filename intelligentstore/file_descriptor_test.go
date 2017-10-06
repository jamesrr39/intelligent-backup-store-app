package intelligentstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewFileInVersion(t *testing.T) {
	fileDescriptor := NewFileInVersion(Hash("abcdef"), "path/to/file")
	assert.Equal(t, Hash("abcdef"), fileDescriptor.Hash)
	assert.Equal(t, "path/to/file", string(fileDescriptor.RelativePath))
}
