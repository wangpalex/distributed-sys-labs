package raft

import (
	"math/rand"
	"time"
)

func GetInitElectionTimeout() time.Duration {
	return time.Duration(rand.Int63()%ElectionTimeout) * time.Millisecond
}

func GetRandElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+rand.Int63()%ElectionTimeout) * time.Millisecond
}

func CloneLogs(src []LogEntry) []LogEntry {
	dst := make([]LogEntry, len(src))
	copy(dst, src)
	return dst
}

func CloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func Min(x, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
}

func Max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}
