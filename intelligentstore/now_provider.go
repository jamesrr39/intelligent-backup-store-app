package intelligentstore

import (
	"time"
)

type nowProvider func() time.Time // TODO move to to goutil

func prodNowProvider() time.Time {
	return time.Now()
}
