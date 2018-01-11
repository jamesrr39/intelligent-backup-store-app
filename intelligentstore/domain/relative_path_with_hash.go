package domain

type RelativePathWithHash struct {
	RelativePath
	Hash
}

func NewRelativePathWithHash(relativePath RelativePath, hash Hash) *RelativePathWithHash {
	return &RelativePathWithHash{relativePath, hash}
}
