package raft

type StableKey string

const (
	keyCurrentTerm  StableKey = "CurrentTerm"
	keyLastVoteTerm StableKey = "LastVoteTerm"
	keyLastVoteCand StableKey = "LastVoteCand"
)

type StableStore interface {
	Set(key StableKey, val uint64) error
	Get(key StableKey) (uint64, error)
}
