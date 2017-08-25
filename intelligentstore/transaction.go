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
	"strings"

	"github.com/jamesrr39/goutil/dirtraversal"
)

type Transaction struct {
	*Revision
	FilesInVersion []*FileDescriptor
}

// TODO: test for >4GB file
func (transaction *Transaction) BackupFile(fileName string, sourceFile io.Reader) error {
	fileName = strings.TrimPrefix(fileName, string(filepath.Separator))

	if dirtraversal.IsTryingToTraverseUp(fileName) {
		return ErrIllegalDirectoryTraversal
	}

	sourceAsBytes, err := ioutil.ReadAll(sourceFile)
	if nil != err {
		return err
	}

	hash, err := NewHash(bytes.NewBuffer(sourceAsBytes))
	if nil != err {
		return err
	}

	filePath := filepath.Join(
		transaction.StoreBasePath,
		".backup_data",
		"objects",
		hash.FirstChunk(),
		hash.Remainder())

	log.Printf("backing up %s into %s\n", fileName, filePath)

	_, err = os.Stat(filePath)
	if nil != err {
		if !os.IsNotExist(err) {
			// permissions issue or something.
			return err
		}
		// file doesn't exist in store already. Write it to store.

		err := os.MkdirAll(filepath.Dir(filePath), 0700)
		if nil != err {
			return err
		}

		log.Printf("writing %s to %s\n", fileName, filePath)
		err = ioutil.WriteFile(filePath, sourceAsBytes, 0700)
		if nil != err {
			return err
		}
	} else {
		// file already exists. Do a byte by byte comparision to make sure there isn't a collision
		existingFile, err := os.Open(filePath)
		if nil != err {
			return fmt.Errorf("couldn't open existing file in store at '%s'. Error: %s", filePath, err)
		}
		defer existingFile.Close()
	}

	transaction.FilesInVersion = append(transaction.FilesInVersion, NewFileInVersion(hash, fileName))

	return nil
}

func (transaction *Transaction) AddAlreadyExistingHash(fileDescriptor *FileDescriptor) (bool, error) {
	isTryingToTraverse := dirtraversal.IsTryingToTraverseUp(string(fileDescriptor.Hash))
	if isTryingToTraverse {
		return false, fmt.Errorf("%s is attempting to traverse up the filesystem tree, which is not allowed (and this is not a hash)", fileDescriptor.Hash)
	}

	bucketsDirPath := filepath.Join(transaction.StoreBasePath, ".backup_data", "objects")

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

func (transaction *Transaction) Commit() error {
	filePath := filepath.Join(transaction.StoreBasePath, ".backup_data", "buckets", transaction.BucketName, "versions", transaction.VersionTimestamp)

	versionContentsFile, err := os.Create(filePath)
	if nil != err {
		return fmt.Errorf("couldn't write version summary file at '%s'. Error: '%s'", filePath, err)
	}
	defer versionContentsFile.Close()

	err = gob.NewEncoder(versionContentsFile).Encode(transaction.FilesInVersion)
	if nil != err {
		return err
	}

	return nil
}
