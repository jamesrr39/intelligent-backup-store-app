package excludesmatcher

import "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"

// FIXME: rename package

type Matcher interface {
	IsIncluded(relativePath intelligentstore.RelativePath) bool
}
