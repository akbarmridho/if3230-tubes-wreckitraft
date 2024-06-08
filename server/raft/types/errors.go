package types

import "errors"

var NodeNetworkError = errors.New("Node is unreachable")

var NotALeaderError = errors.New("Node is not a leader")
