package intelligentstore

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type RegularFileDescriptorWithContents struct {
	Descriptor *RegularFileDescriptor
	Contents   []byte
}

func NewRegularFileDescriptorWithContents(t *testing.T, relativePath RelativePath, modTime time.Time, fileMode os.FileMode, contents []byte) *RegularFileDescriptorWithContents {
	descriptor, err := NewRegularFileDescriptorFromReader(relativePath, modTime, fileMode, bytes.NewBuffer(contents))
	require.Nil(t, err)

	return &RegularFileDescriptorWithContents{descriptor, contents}
}
