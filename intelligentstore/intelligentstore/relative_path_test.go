package intelligentstore

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

func TestNewRelativePathFromFragments(t *testing.T) {
	type args struct {
		fragments []string
	}
	tests := []struct {
		name string
		args args
		want RelativePath
	}{
		{
			name: "multiple",
			args: args{
				fragments: []string{"/a/", "/b/", "/c/", "/d/"},
			},
			want: "a/b/c/d",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewRelativePathFromFragments(tt.args.fragments...); got != tt.want {
				t.Errorf("NewRelativePathFromFragments() = %v, want %v", got, tt.want)
			}
		})
	}
}
