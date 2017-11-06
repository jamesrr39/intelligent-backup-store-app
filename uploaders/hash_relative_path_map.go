package uploaders

import (
	"path/filepath"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/spf13/afero"
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

func BuildRelativePathsWithHashes(fs afero.Fs, backupFromLocation string, requiredRelativePaths []intelligentstore.RelativePath) (HashRelativePathMap, error) {
	hashRelativePathMap := make(HashRelativePathMap)
	for _, requiredRelativePath := range requiredRelativePaths {
		file, err := fs.Open(filepath.Join(backupFromLocation, string(requiredRelativePath)))
		if nil != err {
			return nil, err
		}
		hash, err := intelligentstore.NewHash(file)
		if nil != err {
			return nil, err
		}
		file.Close()

		hashRelativePathMap[hash] = append(hashRelativePathMap[hash], requiredRelativePath)
	}

	return hashRelativePathMap, nil
}
