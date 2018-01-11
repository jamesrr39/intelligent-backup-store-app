package intelligentstore

import "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"

type SearchResult struct {
	RelativePath domain.RelativePath `json:"relativePath"`
	Bucket       *domain.Bucket      `json:"bucket"`
	Revision     *domain.Revision    `json:"revision"`
}
