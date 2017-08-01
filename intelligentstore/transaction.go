package intelligentstore

import (
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jamesrr39/goutil/dirtraversal"
)

type Transaction struct {
	*IntelligentStoreRevision
}

// TODO: test for >4GB file
func (transaction *Transaction) BackupFile(fileName string, sourceFile io.Reader) error {
	if dirtraversal.IsTryingToTraverseUp(fileName) {
		return ErrIllegalDirectoryTraversal
	}

	sourceAsBytes, err := ioutil.ReadAll(sourceFile)
	if nil != err {
		return err
	}

	fileSize := len(sourceAsBytes)

	hasher := sha512.New()
	hasher.Write(sourceAsBytes)
	hash := hex.EncodeToString(hasher.Sum(nil))

	filePath := filepath.Join(transaction.StoreBasePath, ".backup_data", "objects", strconv.Itoa(fileSize), hash)

	log.Printf("backing up %s\n", fileName)

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

		areTheSameBytes := areFilesTheSameBytes(sourceAsBytes, existingFile)
		if !areTheSameBytes {
			log.Printf("DUMP: %v\nsourceAsBytes len: %d\n", transaction.FilesInVersion, len(sourceAsBytes))
			return fmt.Errorf("hash collision detected! new file '%s' and existing file '%s' have the same length and hash but do not have the same bytes", fileName, filePath)
		}
	}

	transaction.FilesInVersion = append(transaction.FilesInVersion, NewFileInVersion(fileSize, hash, fileName))

	return nil
}

func (transaction *Transaction) Commit() error {
	filePath := filepath.Join(transaction.StoreBasePath, ".backup_data", "buckets", transaction.BucketName, "versions", transaction.VersionTimestamp)

	versionContentsFile, err := os.Create(filePath)
	if nil != err {
		return fmt.Errorf("couldn't write version summary file at '%s'. Error: '%s'", filePath, err)
	}
	defer versionContentsFile.Close()

	err = json.NewEncoder(versionContentsFile).Encode(transaction.FilesInVersion)
	if nil != err {
		return err
	}

	return nil
}
