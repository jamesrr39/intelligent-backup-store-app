package intelligentstore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_BackupFile(t *testing.T) {
	transaction := &Transaction{}
	byteBuffer := bytes.NewBuffer(nil)

	err := transaction.BackupFile("../a.txt", byteBuffer)

	assert.Error(t, err)
	assert.Equal(t, ErrIllegalDirectoryTraversal, err)
}
