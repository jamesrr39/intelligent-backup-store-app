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
	"sync"

	"github.com/jamesrr39/goutil/dirtraversal"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type Transaction struct {
	*Revision
	FilesInVersion                  []*FileDescriptor
	isFileScheduledForUploadAlready map[Hash]bool
	mu                              *sync.RWMutex
}

func NewTransaction(revision *Revision, fileDescriptors []*FileDescriptor) (*Transaction, error) {
	tx := &Transaction{revision, nil, make(map[Hash]bool), &sync.RWMutex{}}
	for _, fileDescriptor := range fileDescriptors {
		if dirtraversal.IsTryingToTraverseUp(string(fileDescriptor.RelativePath)) {
			return nil, ErrIllegalDirectoryTraversal
		}

		err := tx.addDescriptorToTransaction(fileDescriptor)
		if nil != err {
			return nil, errors.Wrapf(err, "couldn't add file %v to transaction", fileDescriptor)
		}
	}
	return tx, nil
}

var ErrFileNotRequiredForTransaction = errors.New("hash is not scheduled for upload, or has already been uploaded")

// TODO: test for >4GB file
func (transaction *Transaction) BackupFile(sourceFile io.Reader) error {
	sourceAsBytes, err := ioutil.ReadAll(sourceFile)
	if nil != err {
		return err
	}

	hash, err := NewHash(bytes.NewBuffer(sourceAsBytes))
	if nil != err {
		return err
	}

	if !transaction.isFileScheduledForUploadAlready[hash] {
		return ErrFileNotRequiredForTransaction
	}

	filePath := filepath.Join(
		transaction.StoreBasePath,
		".backup_data",
		"objects",
		hash.FirstChunk(),
		hash.Remainder())

	log.Printf("backing up %s into %s\n", hash, filePath)

	_, err = transaction.fs.Stat(filePath)
	if nil != err {
		if !os.IsNotExist(err) {
			// permissions issue or something.
			return err
		}
		// file doesn't exist in store already. Write it to store.

		err := transaction.fs.MkdirAll(filepath.Dir(filePath), 0700)
		if nil != err {
			return err
		}

		log.Printf("writing %s to %s\n", hash, filePath)
		err = afero.WriteFile(transaction.fs, filePath, sourceAsBytes, 0700)
		if nil != err {
			return err
		}
	} else {
		// file already exists. Do a byte by byte comparision to make sure there isn't a collision
		existingFile, err := transaction.fs.Open(filePath)
		if nil != err {
			return fmt.Errorf("couldn't open existing file in store at '%s'. Error: %s", filePath, err)
		}
		defer existingFile.Close()
	}

	transaction.mu.Lock()
	delete(transaction.isFileScheduledForUploadAlready, hash)
	transaction.mu.Unlock()

	return nil
}

// AddDescriptorToTransaction adds a descriptor to the transaction
// returns (is_file_needed, err)
func (transaction *Transaction) addDescriptorToTransaction(fileDescriptor *FileDescriptor) error {
	isTryingToTraverse := dirtraversal.IsTryingToTraverseUp(string(fileDescriptor.Hash))
	if isTryingToTraverse {
		return fmt.Errorf("%s is attempting to traverse up the filesystem tree, which is not allowed (and this is not a hash)", fileDescriptor.Hash)
	}

	transaction.FilesInVersion = append(transaction.FilesInVersion, fileDescriptor)

	// check if it's scheduled for upload already
	transaction.mu.Lock()
	defer transaction.mu.Unlock()

	isFileScheduledForUploadAlready := transaction.isFileScheduledForUploadAlready[fileDescriptor.Hash]
	if isFileScheduledForUploadAlready {
		return nil
	}

	// check if the file exists on disk
	bucketsDirPath := filepath.Join(transaction.StoreBasePath, ".backup_data", "objects")

	filePath := filepath.Join(bucketsDirPath, fileDescriptor.Hash.FirstChunk(), fileDescriptor.Hash.Remainder())
	_, err := transaction.IntelligentStore.fs.Stat(filePath)
	if nil != err {
		if os.IsNotExist(err) {
			transaction.isFileScheduledForUploadAlready[fileDescriptor.Hash] = true
			return nil
		}
		return fmt.Errorf("couldn't detect if %s is already in the index. Error: %s", fileDescriptor.Hash, err)
	}

	// file on disk was successfully stat'ed (and exists)
	return nil
}

// Commit closes the transaction and writes the revision data to disk
func (transaction *Transaction) Commit() error {
	amountOfFilesRemainingToUpload := len(transaction.isFileScheduledForUploadAlready)
	if amountOfFilesRemainingToUpload > 0 {
		log.Println("remaining files:")
		for hash, isScheduledForUpload := range transaction.isFileScheduledForUploadAlready {
			log.Printf("hash: %s, %v\n", hash, isScheduledForUpload)
		}
		return fmt.Errorf(
			"tried to commit the transaction but there are %d files left to upload",
			amountOfFilesRemainingToUpload)
	}

	filePath := filepath.Join(
		transaction.Revision.Bucket.bucketPath(),
		"versions",
		strconv.FormatInt(int64(transaction.VersionTimestamp), 10))

	versionContentsFile, err := transaction.fs.Create(filePath)
	if nil != err {
		return fmt.Errorf("couldn't write version summary file at '%s'. Error: '%s'", filePath, err)
	}
	defer versionContentsFile.Close()

	err = gob.NewEncoder(versionContentsFile).Encode(transaction.FilesInVersion)
	if nil != err {
		return err
	}

	err = versionContentsFile.Sync()
	if nil != err {
		return errors.Wrap(err, "couldn't sync the version contents file")
	}

	err = transaction.IntelligentStore.removeStoreLock()
	if nil != err {
		return errors.Wrap(err, "couldn't remove lock file")
	}

	return nil
}

func (transaction *Transaction) Rollback() error {
	err := transaction.IntelligentStore.removeStoreLock()
	if nil != err {
		return errors.Wrap(err, "couldn't remove lock file")
	}

	return nil
}

func (transaction *Transaction) GetHashesForRequiredContent() []Hash {
	var hashes []Hash

	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	for hash := range transaction.isFileScheduledForUploadAlready {
		hashes = append(hashes, hash)
	}

	return hashes
}
