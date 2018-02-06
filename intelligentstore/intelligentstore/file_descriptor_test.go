package intelligentstore

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const fileMode600 os.FileMode = (1 << 8) + (1 << 7)

func Test_NewRegularFileDescriptor(t *testing.T) {
	fileDescriptor := NewRegularFileDescriptor(NewFileInfo(FileTypeRegular, "path/to/file", time.Unix(0, 0), 0, fileMode600), Hash("abcdef"))
	assert.Equal(t, Hash("abcdef"), fileDescriptor.Hash)
	assert.Equal(t, "path/to/file", string(fileDescriptor.RelativePath))
}
