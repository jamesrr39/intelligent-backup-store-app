package storefs

import (
	"os"
)

type MockFs struct {
	CreateFunc    func(name string) (*os.File, error)
	RemoveFunc    func(path string) error
	RemoveAllFunc func(path string) error
	StatFunc      func(path string) (os.FileInfo, error)
	ReadDirFunc   func(dirname string) ([]os.FileInfo, error)
	MkdirFunc     func(path string, perm os.FileMode) error
	MkdirAllFunc  func(path string, perm os.FileMode) error
	OpenFunc      func(path string) (*os.File, error)
	WriteFileFunc func(path string, data []byte, perm os.FileMode) error
	RenameFunc    func(old, new string) error
	OpenFileFunc  func(name string, flag int, perm os.FileMode) (*os.File, error)
	SymlinkFunc   func(oldname, newname string) error
	ChmodFunc     func(name string, mode os.FileMode) error
}

func NewMockFs() MockFs {
	return MockFs{}
}

func (fs MockFs) Create(name string) (*os.File, error) {
	return fs.CreateFunc(name)
}
func (fs MockFs) Remove(path string) error {
	return fs.RemoveFunc(path)
}
func (fs MockFs) RemoveAll(path string) error {
	return fs.RemoveAllFunc(path)
}
func (fs MockFs) Stat(path string) (os.FileInfo, error) {
	return fs.StatFunc(path)
}
func (fs MockFs) ReadDir(path string) ([]os.FileInfo, error) {
	return fs.ReadDirFunc(path)
}
func (fs MockFs) Mkdir(path string, perm os.FileMode) error {
	return fs.MkdirFunc(path, perm)
}

func (fs MockFs) MkdirAll(path string, perm os.FileMode) error {
	return fs.MkdirAllFunc(path, perm)
}

func (fs MockFs) Open(name string) (*os.File, error) {
	return fs.OpenFunc(name)
}
func (fs MockFs) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return fs.OpenFileFunc(name, flag, perm)
}

func (fs MockFs) WriteFile(path string, data []byte, perm os.FileMode) error {
	return fs.WriteFileFunc(path, data, perm)
}
func (fs MockFs) Rename(old, new string) error {
	return fs.RenameFunc(old, new)
}
func (fs MockFs) Symlink(oldName, newName string) error {
	return fs.SymlinkFunc(oldName, newName)
}
func (fs MockFs) Chmod(name string, mode os.FileMode) error {
	return fs.ChmodFunc(name, mode)
}
