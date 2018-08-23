package dal

import (
	"time"
)

type NowProvider func() time.Time // TODO move to to goutil

func prodNowProvider() time.Time {
	return time.Now()
}
