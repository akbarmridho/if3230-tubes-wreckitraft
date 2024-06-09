package types

import "errors"

var NodeNetworkError = errors.New("node is unreachable")

var NotALeaderError = errors.New("node is not a leader")
