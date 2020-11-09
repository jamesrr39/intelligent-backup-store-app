package dal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
)

type LockDAL struct {
	storeDAL *IntelligentStoreDAL
}

// GetLockInformation gets the information about the current Lock on the Store, if any.
// It returns
// - (*StoreLock, nil) if there is a lock
// - (nil, nil) if there is currently no lock
// - (nil, error) for any error
func (s *LockDAL) GetLockInformation() (*StoreLock, error) {
	file, err := s.storeDAL.fs.Open(s.getLockFilePath())
	if nil != err {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var storeLock *StoreLock
	err = json.NewDecoder(file).Decode(&storeLock)
	if nil != err {
		return nil, err
	}

	return storeLock, nil
}

var ErrLockAlreadyTaken = errors.New("lock already taken")

func (s *LockDAL) acquireStoreLock(text string) (*StoreLock, errorsx.Error) {
	_, err := s.storeDAL.fs.Stat(s.getLockFilePath())
	if nil == err {
		return nil, errorsx.Wrap(ErrLockAlreadyTaken)
	}
	if !os.IsNotExist(err) {
		return nil, errorsx.Wrap(err)
	}

	//lockFile, err := s.fs.OpenFile(s.getLockFilePath(), os.O_CREATE, 0600)
	lockFile, err := s.storeDAL.fs.OpenFile(s.getLockFilePath(), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if nil != err {
		return nil, errorsx.Wrap(err)
	}
	defer lockFile.Close()

	lock := &StoreLock{
		time.Now(),
		os.Getpid(),
		text,
	}

	err = json.NewEncoder(lockFile).Encode(lock)
	if nil != err {
		return nil, errorsx.Wrap(err)
	}

	return lock, nil
}

func (s *LockDAL) removeStoreLock() errorsx.Error {
	err := s.storeDAL.fs.RemoveAll(s.getLockFilePath())
	if err != nil {
		return errorsx.Wrap(err)
	}
	return nil
}

func (s *LockDAL) getLockFilePath() string {
	return filepath.Join(s.storeDAL.StoreBasePath, ".backup_data", "locks", "store_lock.txt")
}

type StoreLock struct {
	AcquisitionTime time.Time `json:"acquisitionTime"`
	Pid             int       `json:"pid"`
	Text            string    `json:"text"`
}
