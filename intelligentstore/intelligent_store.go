package intelligentstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/jamesrr39/goutil/userextra"
)

/*
- {root}
  - .backup_data
	- versions
	  - {timestamp}
  	- objects
	  - {file_length}
		- {file_sha512}
*/
type IntelligentStore struct {
	FullPathToBase string
}

func NewIntelligentStoreConnToExisting(pathToBase string) (*IntelligentStore, error) {
	fullPath, err := expandPath(pathToBase)
	if nil != err {
		return nil, err
	}

	fileInfo, err := os.Stat(filepath.Join(fullPath, ".backup_data"))
	if nil != err {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("IntelligentStore not initialised yet. Use init to create a new store")
		}
		return nil, err
	}

	if !fileInfo.IsDir() {
		return nil, fmt.Errorf("store data directory is not a directory (either wrong path, or corrupted)")
	}

	return &IntelligentStore{fullPath}, nil
}

func CreateIntelligentStoreAndNewConn(pathToBase string) (*IntelligentStore, error) {
	fullPath, err := expandPath(pathToBase)

	fileInfos, err := ioutil.ReadDir(fullPath)
	if nil != err {
		return nil, fmt.Errorf("couldn't get a file listing for '%s'. Error: '%s'", fullPath, err)
	}

	if 0 != len(fileInfos) {
		return nil, fmt.Errorf("'%s' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there", fullPath)
	}

	versionsFolderPath := filepath.Join(fullPath, ".backup_data", "versions")
	err = os.MkdirAll(versionsFolderPath, 0700)
	if nil != err {
		return nil, fmt.Errorf("couldn't create data folder for backup versions at '%s'. Error: '%s'", versionsFolderPath, err)
	}

	objectsFolderPath := filepath.Join(fullPath, ".backup_data", "objects")
	err = os.MkdirAll(objectsFolderPath, 0700)
	if nil != err {
		return nil, fmt.Errorf("couldn't create data folder for backup objects at '%s'. Error: '%s'", objectsFolderPath, err)
	}

	return &IntelligentStore{fullPath}, nil
}

func (r *IntelligentStore) Begin() *IntelligentStoreVersion {
	versionTimestamp := time.Now().Format("2006-01-02_15-04-05")

	return &IntelligentStoreVersion{r, versionTimestamp, nil}
}

func expandPath(pathToBase string) (string, error) {
	userExpandedPath, err := userextra.ExpandUser(pathToBase)
	if nil != err {
		return "", err
	}

	fullPath, err := filepath.Abs(userExpandedPath)
	if nil != err {
		return "", err
	}

	return fullPath, nil
}
