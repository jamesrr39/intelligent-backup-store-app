package excludesmatcher

import (
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/stretchr/testify/assert"
)

func Test_SinglePrefixMatcher_IsIncluded(t *testing.T) {
	matcher := NewSimplePrefixMatcher("folder-1/b/a")

	assert.True(t, matcher.IsIncluded(domain.RelativePath("folder-1/b/a")))
	assert.True(t, matcher.IsIncluded(domain.RelativePath("folder-1/b/ab")))
	assert.True(t, matcher.IsIncluded(domain.RelativePath("folder-1/b/a/345")))

	assert.False(t, matcher.IsIncluded(domain.RelativePath("folder-1/b")))
}
