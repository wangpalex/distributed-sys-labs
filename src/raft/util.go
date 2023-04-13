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
