package intelligentstore

import (
	"bytes"
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func Test_BackupFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)

	transactionDAL := NewTransactionDAL(mockStore.IntelligentStoreDAL)
	byteBuffer := bytes.NewBuffer(nil)

	transaction := &domain.Transaction{}

	err := transactionDAL.BackupFile(transaction, "../a.txt", byteBuffer)

	assert.Error(t, err)
	assert.Equal(t, ErrIllegalDirectoryTraversal, err)
}
