package uploaders

import (
	"log"
	"log/slog"
	"path/filepath"
	"time"

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
	totalRequiredHashes := len(requiredRelativePaths)
	log.Printf("%d relative paths required\n", totalRequiredHashes)


	// TODO inject channel into function
	go func() {
		for {
			time.Sleep(time.Second * 5)
			totalCalculated := len(hashRelativePathMap)
			slog.Info("calculating hashes",
				"total calculated", totalCalculated,
				"total", totalRequiredHashes,
				"progress %", (float64(totalCalculated) * 100 / float64(totalRequiredHashes)),
			)
		}
	}()

	for _, requiredRelativePath := range requiredRelativePaths {

		filePath := filepath.Join(backupFromLocation, string(requiredRelativePath))

		hash, err := calculateHash(fs, filePath)
		if nil != err {
			return nil, errorsx.Wrap(err, "filePath", filePath)
		}

		hashRelativePathMap[hash] = append(hashRelativePathMap[hash], requiredRelativePath)
	}

	return hashRelativePathMap, nil
}

func calculateHash(fs gofs.Fs, filePath string) (intelligentstore.Hash, errorsx.Error) {
	file, err := fs.Open(filePath)
	if nil != err {
		return "", errorsx.Wrap(err)
	}
	defer file.Close()

	hash, err := intelligentstore.NewHash(file)
	if nil != err {
		return "", errorsx.Wrap(err)
	}

	return hash, nil
}
