package intelligentstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_NewRegularFileDescriptor(t *testing.T) {
	fileDescriptor := NewRegularFileDescriptor(NewFileInfo("path/to/file", time.Unix(0, 0), 0), Hash("abcdef"))
	assert.Equal(t, Hash("abcdef"), fileDescriptor.Hash)
	assert.Equal(t, "path/to/file", string(fileDescriptor.RelativePath))
}
