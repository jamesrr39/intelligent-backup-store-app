package intelligentstore

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/stretchr/testify/require"
)

type RegularFileDescriptorWithContents struct {
	Descriptor *domain.RegularFileDescriptor
	Contents   []byte
}

func NewRegularFileDescriptorWithContents(t *testing.T, relativePath domain.RelativePath, modTime time.Time, fileMode os.FileMode, contents []byte) *RegularFileDescriptorWithContents {
	descriptor, err := domain.NewRegularFileDescriptorFromReader(relativePath, modTime, fileMode, bytes.NewBuffer(contents))
	require.Nil(t, err)

	return &RegularFileDescriptorWithContents{descriptor, contents}
}
