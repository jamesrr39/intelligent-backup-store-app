package excludesmatcher

import (
	"bufio"
	"io"
	"strings"

	"github.com/gobwas/glob"
)

// ExcludesMatcher is a type that matches file names against excluded names
type ExcludesMatcher struct {
	globs []glob.Glob
}

// NewExcludesMatcherFromReader creates a new ExcludesMatcher from a reader
func NewExcludesMatcherFromReader(reader io.Reader) (*ExcludesMatcher, error) {
	var matcherPatterns []glob.Glob

	bufScanner := bufio.NewScanner(reader)
	for bufScanner.Scan() {
		err := bufScanner.Err()
		if nil != err {
			return nil, err
		}
		pattern := strings.TrimSpace(bufScanner.Text())
		if pattern == "" {
			continue
		}

		if strings.HasPrefix(pattern, "#") {
			// line is a comment
			continue
		}

		matcher, err := glob.Compile(pattern)
		if nil != err {
			return nil, err
		}

		matcherPatterns = append(matcherPatterns, matcher)
	}

	return &ExcludesMatcher{
		globs: matcherPatterns,
	}, nil
}

func (e *ExcludesMatcher) Matches(text string) bool {
	for _, matcherGlob := range e.globs {
		doesMatch := matcherGlob.Match(text)

		if doesMatch {
			return true
		}
	}
	return false
}
