package intelligentstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jamesrr39/goutil/dirtraversal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type TransactionDAL struct {
	*IntelligentStoreDAL
}

func NewTransactionDAL(intelligentStoreDAL *IntelligentStoreDAL) *TransactionDAL {
	return &TransactionDAL{intelligentStoreDAL}
}

// TODO: test for >4GB file
func (dal *TransactionDAL) BackupFile(transaction *domain.Transaction, fileName string, sourceFile io.Reader) error {
	fileName = strings.TrimPrefix(fileName, string(filepath.Separator))

	if dirtraversal.IsTryingToTraverseUp(fileName) {
		return ErrIllegalDirectoryTraversal
	}

	sourceAsBytes, err := ioutil.ReadAll(sourceFile)
	if nil != err {
		return err
	}

	hash, err := domain.NewHash(bytes.NewBuffer(sourceAsBytes))
	if nil != err {
		return err
	}

	filePath := filepath.Join(
		dal.StoreBasePath,
		".backup_data",
		"objects",
		hash.FirstChunk(),
		hash.Remainder())

	log.Printf("backing up %s into %s\n", fileName, filePath)

	_, err = dal.fs.Stat(filePath)
	if nil != err {
		if !os.IsNotExist(err) {
			// permissions issue or something.
			return err
		}
		// file doesn't exist in store already. Write it to store.

		err := dal.fs.MkdirAll(filepath.Dir(filePath), 0700)
		if nil != err {
			return err
		}

		log.Printf("writing %s to %s\n", fileName, filePath)
		err = afero.WriteFile(dal.fs, filePath, sourceAsBytes, 0700)
		if nil != err {
			return err
		}
	} else {
		existingFile, err := dal.fs.Open(filePath)
		if nil != err {
			return fmt.Errorf("couldn't open existing file in store at '%s'. Error: %s", filePath, err)
		}
		defer existingFile.Close()
	}

	transaction.FilesInVersion = append(
		transaction.FilesInVersion,
		domain.NewFileInVersion(hash, domain.NewRelativePath(fileName)))

	return nil
}

func (dal *TransactionDAL) AddAlreadyExistingHash(transaction *domain.Transaction, fileDescriptor *domain.FileDescriptor) (bool, error) {
	isTryingToTraverse := dirtraversal.IsTryingToTraverseUp(string(fileDescriptor.Hash))
	if isTryingToTraverse {
		return false, fmt.Errorf("%s is attempting to traverse up the filesystem tree, which is not allowed (and this is not a hash)", fileDescriptor.Hash)
	}

	bucketsDirPath := filepath.Join(dal.StoreBasePath, ".backup_data", "objects")

	filePath := filepath.Join(bucketsDirPath, fileDescriptor.Hash.FirstChunk(), fileDescriptor.Hash.Remainder())
	_, err := os.Stat(filePath)
	if nil != err {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("couldn't detect if %s is already in the index. Error: %s", fileDescriptor.Hash, err)
	}

	// the hash already exists, we can add it to the transaction
	transaction.FilesInVersion = append(transaction.FilesInVersion, fileDescriptor)
	return true, nil
}

func (dal *TransactionDAL) Commit(transaction *domain.Transaction) error {
	filePath := filepath.Join(
		dal.StoreBasePath,
		".backup_data",
		"buckets",
		transaction.BucketName,
		"versions",
		strconv.FormatInt(int64(transaction.VersionTimestamp), 10))

	versionContentsFile, err := dal.fs.Create(filePath)
	if nil != err {
		return fmt.Errorf("couldn't write version summary file at '%s'. Error: '%s'", filePath, err)
	}
	defer versionContentsFile.Close()

	log.Printf("files written to version: %v\n", transaction.FilesInVersion)

	err = gob.NewEncoder(versionContentsFile).Encode(transaction.FilesInVersion)
	if nil != err {
		return err
	}

	err = versionContentsFile.Sync()
	if nil != err {
		return errors.Wrap(err, "couldn't sync the version contents file")
	}

	return nil
}
