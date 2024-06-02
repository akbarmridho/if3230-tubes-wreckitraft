package util

import (
	"math/rand"
	"time"
)

func RandomTimeout(min, max time.Duration) time.Duration {
	return time.Duration(rand.Int63n(int64(max-min+1))) + min
}
