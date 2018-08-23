package storefs

import (
	"io/ioutil"
	"os"
)

type Fs interface {
	Create(name string) (*os.File, error)
	Remove(path string) error
	RemoveAll(path string) error
	Stat(path string) (os.FileInfo, error)
	ReadDir(dirname string) ([]os.FileInfo, error)
	Mkdir(path string, perm os.FileMode) error
	MkdirAll(path string, perm os.FileMode) error
	Open(path string) (*os.File, error)
	WriteFile(path string, data []byte, perm os.FileMode) error
	Rename(old, new string) error
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	Symlink(oldName, newName string) error
	Chmod(name string, mode os.FileMode) error
}

type OsFs struct{}

func NewOsFs() *OsFs {
	return &OsFs{}
}

func (fs *OsFs) Create(name string) (*os.File, error) {
	return os.Create(name)
}
func (fs *OsFs) Remove(path string) error {
	return os.Remove(path)
}
func (fs *OsFs) RemoveAll(path string) error {
	return os.RemoveAll(path)
}
func (fs *OsFs) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}
func (fs *OsFs) ReadDir(path string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(path)
}
func (fs *OsFs) Mkdir(path string, perm os.FileMode) error {
	return os.Mkdir(path, perm)
}

func (fs *OsFs) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *OsFs) Open(name string) (*os.File, error) {
	return os.Open(name)
}
func (fs *OsFs) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (fs *OsFs) WriteFile(path string, data []byte, perm os.FileMode) error {
	return ioutil.WriteFile(path, data, perm)
}
func (fs *OsFs) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}
func (fs *OsFs) Symlink(oldname, newname string) error {
	return os.Symlink(oldname, newname)
}
func (fs *OsFs) Chmod(name string, mode os.FileMode) error {
	return os.Chmod(name, mode)
}
