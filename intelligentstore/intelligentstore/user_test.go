package intelligentstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewUser(t *testing.T) {
	u := NewUser(3, "test å user", "test@example.com")

	assert.Equal(t, int64(3), u.ID)
	assert.Equal(t, "test å user", u.DisplayName)
}
