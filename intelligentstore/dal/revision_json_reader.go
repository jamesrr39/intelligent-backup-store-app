package dal

import (
	"encoding/json"
	"io"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

var (
	_ revisionReader = &revisionJSONReader{}
)

type revisionJSONReader struct {
	revisionFile io.ReadSeekCloser
}

func (r *revisionJSONReader) ReadDir(searchPath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error) {
	iterator, err := r.Iterator()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return iteratorReadDir(iterator, searchPath)
}

func (r *revisionJSONReader) Stat(searchPath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error) {
	iterator, err := r.Iterator()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return iteratorStat(iterator, searchPath)
}

type JSONIterator struct {
	jsonMessages []json.RawMessage
	currentIndex int
}

func (r *revisionJSONReader) Iterator() (Iterator, errorsx.Error) {
	_, err := r.revisionFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	var fdBytes []json.RawMessage
	err = json.NewDecoder(r.revisionFile).Decode(&fdBytes)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return &JSONIterator{jsonMessages: fdBytes, currentIndex: -1}, nil
}

func (r *JSONIterator) Next() bool {
	if r.currentIndex >= (len(r.jsonMessages) - 1) {
		return false
	}

	r.currentIndex++
	return true
}

func (r *JSONIterator) Scan() (intelligentstore.FileDescriptor, errorsx.Error) {
	fdJSON := r.jsonMessages[r.currentIndex]

	var fileInfo intelligentstore.FileInfo
	err := json.Unmarshal(fdJSON, &fileInfo)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	var objToUnmarshalTo intelligentstore.FileDescriptor
	switch fileInfo.Type {
	case intelligentstore.FileTypeRegular:
		objToUnmarshalTo = &intelligentstore.RegularFileDescriptor{}
	case intelligentstore.FileTypeSymlink:
		objToUnmarshalTo = &intelligentstore.SymlinkFileDescriptor{}
	default:
		return nil, errorsx.Errorf("unrecognised file descriptor type. JSON: %q", string(fdJSON))
	}
	err = json.Unmarshal(fdJSON, &objToUnmarshalTo)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return objToUnmarshalTo, nil
}

func (r *JSONIterator) Err() errorsx.Error {
	return nil
}

func (r *revisionJSONReader) Close() errorsx.Error {
	return errorsx.Wrap(r.revisionFile.Close())
}
