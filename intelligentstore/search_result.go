package intelligentstore

type SearchResult struct {
	RelativePath RelativePath `json:"relativePath"`
	Bucket       *Bucket      `json:"bucket"`
	Revision     *Revision    `json:"revision"`
}
