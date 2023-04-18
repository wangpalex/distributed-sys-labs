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

func cloneLogs(src []LogEntry) []LogEntry {
	dst := make([]LogEntry, len(src))
	copy(dst, src)
	return dst
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func min(x, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
}

func max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}
