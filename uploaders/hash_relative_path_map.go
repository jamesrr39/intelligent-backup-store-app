package uploaders

import (
	"log"
	"path/filepath"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

type HashRelativePathMap map[intelligentstore.Hash][]intelligentstore.RelativePath

func (m HashRelativePathMap) ToSlice() []*intelligentstore.RelativePathWithHash {
	var relativePathsWithHashes []*intelligentstore.RelativePathWithHash
	for hash, relativePaths := range m {
		for _, relativePath := range relativePaths {
			relativePathWithHash := &intelligentstore.RelativePathWithHash{
				RelativePath: relativePath,
				Hash:         hash,
			}
			relativePathsWithHashes = append(relativePathsWithHashes, relativePathWithHash)
		}
	}
	return relativePathsWithHashes
}

func BuildRelativePathsWithHashes(fs gofs.Fs, backupFromLocation string, requiredRelativePaths []intelligentstore.RelativePath) (HashRelativePathMap, errorsx.Error) {
	hashRelativePathMap := make(HashRelativePathMap)
	log.Printf("%d relative paths required\n", len(requiredRelativePaths))
	for _, requiredRelativePath := range requiredRelativePaths {
		filePath := filepath.Join(backupFromLocation, string(requiredRelativePath))
		file, err := fs.Open(filePath)
		if nil != err {
			return nil, errorsx.Wrap(err, "filePath", filePath)
		}
		hash, err := intelligentstore.NewHash(file)
		if nil != err {
			return nil, errorsx.Wrap(err, "filePath", filePath)
		}
		file.Close()

		hashRelativePathMap[hash] = append(hashRelativePathMap[hash], requiredRelativePath)
	}

	return hashRelativePathMap, nil
}
