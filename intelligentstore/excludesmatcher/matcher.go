package excludesmatcher

import "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"

// FIXME: rename package

type Matcher interface {
	IsIncluded(relativePath domain.RelativePath) bool
}
