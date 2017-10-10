package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewRelativePath(t *testing.T) {
	// windows
	assert.Equal(t, "a\\b.txt", string(NewRelativePath("\\a\\b.txt")))
	assert.Equal(t, "a\\b.txt", string(NewRelativePath("a\\b.txt")))

	// everyone else
	assert.Equal(t, "a/b.txt", string(NewRelativePath("/a/b.txt")))
	assert.Equal(t, "a/b.txt", string(NewRelativePath("a/b.txt")))
}
