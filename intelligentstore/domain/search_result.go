package domain

type SearchResult struct {
	RelativePath RelativePath `json:"relativePath"`
	Bucket       *Bucket      `json:"bucket"`
	Revision     *Revision    `json:"revision"`
}

func NewSearchResult(relativePath RelativePath, bucket *Bucket, revision *Revision) *SearchResult {
	return &SearchResult{relativePath, bucket, revision}
}
