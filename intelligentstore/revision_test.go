package intelligentstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Revision_String(t *testing.T) {
	assert.Equal(t, "1407378831", RevisionVersion(1407378831).String())
	assert.Equal(t, "1507381831", RevisionVersion(1507381831).String())
	assert.Equal(t, "1607361831", RevisionVersion(1607361831).String())
	assert.Equal(t, "1707471831", RevisionVersion(1707471831).String())
	assert.Equal(t, "1807471831", RevisionVersion(1807471831).String())
}
