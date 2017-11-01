package intelligentstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_NewFileInVersion(t *testing.T) {
	fileDescriptor := NewFileInVersion(NewFileInfo("path/to/file", time.Unix(0, 0), 0), Hash("abcdef"))
	assert.Equal(t, Hash("abcdef"), fileDescriptor.Hash)
	assert.Equal(t, "path/to/file", string(fileDescriptor.RelativePath))
}
