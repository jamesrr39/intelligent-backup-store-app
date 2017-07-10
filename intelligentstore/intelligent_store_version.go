package intelligentstore

import (
	"bufio"
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jamesrr39/goutil/dirtraversal"
)

type IntelligentStoreVersion struct {
	*IntelligentStoreBucket
	versionTimestamp string
	filesInVersion   []*FileInVersion
}

// TODO: test for >4GB file
func (r *IntelligentStoreVersion) BackupFile(fileName string, sourceFile io.Reader) error {
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

	filePath := filepath.Join(r.StoreBasePath, ".backup_data", "objects", strconv.Itoa(fileSize), hash)

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

		posInNewFile := 0
		existingFileBuf := bufio.NewScanner(existingFile)
		for existingFileBuf.Scan() {
			existingFilePassBytes := existingFileBuf.Bytes()
			lenPassBytes := len(existingFilePassBytes)
			newFilePassBytes := sourceAsBytes[posInNewFile : posInNewFile+lenPassBytes]
			if !bytes.Equal(existingFilePassBytes, newFilePassBytes) {
				return fmt.Errorf("hash collision detected! new file '%s' and existing file '%s' have the same length and hash but do not have the same bytes", fileName, filePath)
			}

			posInNewFile += lenPassBytes
		}
	}

	r.filesInVersion = append(r.filesInVersion, NewFileInVersion(fileSize, hash, fileName))

	return nil
}

func (r *IntelligentStoreVersion) Commit() error {
	filePath := filepath.Join(r.StoreBasePath, ".backup_data", "buckets", r.IntelligentStoreBucket.BucketName, "versions", r.versionTimestamp)

	versionContentsFile, err := os.Create(filePath)
	if nil != err {
		return fmt.Errorf("couldn't write version summary file at '%s'. Error: '%s'", filePath, err)
	}
	defer versionContentsFile.Close()

	err = json.NewEncoder(versionContentsFile).Encode(r.filesInVersion)
	if nil != err {
		return err
	}

	return nil
}

func (r *IntelligentStoreVersion) String() string {
	return "versionedLocalRepository:" + r.StoreBasePath
}
