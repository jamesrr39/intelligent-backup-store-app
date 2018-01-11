package excludesmatcher

import (
	"strings"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
)

type SimplePrefixMatcher struct {
	Prefix string
}

func NewSimplePrefixMatcher(prefix string) *SimplePrefixMatcher {
	return &SimplePrefixMatcher{prefix}
}

func (m *SimplePrefixMatcher) IsIncluded(relativePath domain.RelativePath) bool {
	return strings.HasPrefix(string(relativePath), m.Prefix)
}
