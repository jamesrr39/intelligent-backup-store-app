package intelligentstore

import (
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
	*IntelligentStore
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

	filePath := filepath.Join(r.FullPathToBase, ".backup_data", "objects", strconv.Itoa(fileSize), hash)

	_, err = os.Stat(filePath)
	if nil != err {
		if !os.IsNotExist(err) {
			return err
		}

		err := os.MkdirAll(filepath.Dir(filePath), 0700)
		if nil != err {
			return err
		}

		err = ioutil.WriteFile(filePath, sourceAsBytes, 0700)
		if nil != err {
			return err
		}
	}

	r.filesInVersion = append(r.filesInVersion, NewFileInVersion(fileSize, hash, filePath))

	return nil
}

func (r *IntelligentStoreVersion) Commit() error {

	err := os.MkdirAll(filepath.Join(r.IntelligentStore.FullPathToBase, ".backup_data", "versions", r.versionTimestamp), 0700)
	if nil != err {
		return err
	}

	filePath := filepath.Join(r.FullPathToBase, ".backup_data", "versions", r.versionTimestamp)

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
	return "versionedLocalRepository:" + r.FullPathToBase
}
