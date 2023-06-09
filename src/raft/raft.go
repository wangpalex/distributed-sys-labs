package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// rf.Snapshot(index int, snapshot []byte)
//   ask a Raft to save the snapshot and discard logs preceding snapshot index
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	// In milliseconds
	ElectionTimeout   = 800
	HeartbeatInterval = 100
)

const (
	Leader = iota
	Candidate
	Follower
)

type ApplyMsg struct {
	CommandValid bool // True if this msg is for newly committed log entry
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool // True if this msg is for snapshot
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Synchronization
	mu        sync.RWMutex  // Lock to protect shared access to this peer's state
	applyCh   chan ApplyMsg // Channel to send applied entries or snapshot
	applyCond *sync.Cond    // Condition variable to signal log apply

	// Persistent states
	currTerm      int        // Current term known
	votedFor      int        // Peer id this raft has voted for current term
	logs          []LogEntry // Log entries
	snapshotIndex int        // Last included log index in snapshot
	snapshotTerm  int        // Corresponding term of log at snapshotIndex
	snapshot      []byte     // Snapshot Data

	// Volatile states
	role        int   // My role
	commitIndex int   // Highest log index known to be committed
	lastApplied int   // Highest log index applied to state machine
	nextIndex   []int // Index of next log entry to send to that server
	matchIndex  []int // Highest log index known to be replicated on that server

	// Timers
	electionTimer   *time.Timer
	heartbeatTimers []*time.Timer
	stopHeartbeat   chan struct{}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.mu = sync.RWMutex{}
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.currTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1) // Log starts at index 1
	rf.role = Follower
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	rf.heartbeatTimers = make([]*time.Timer, len(rf.peers))
	for p := range rf.peers {
		timer := time.NewTimer(HeartbeatInterval * time.Millisecond)
		timer.Stop() // Restart when elected as leader
		rf.heartbeatTimers[p] = timer
	}
	rf.stopHeartbeat = make(chan struct{})

	// Initialize from state persisted before a crash
	rf.readPersist()
	// Set commit index and last applied index to at least snapshot index
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex

	// Debug(dTrace, "%v: snpIdx=%v, snpTerm=%v, logs %+v", rf.getIdAndRole(), rf.snapshotIndex, rf.snapshotTerm, rf.logs)

	rf.electionTimer = time.NewTimer(GetInitElectionTimeout())
	// Start ticker goroutine to start elections
	go rf.ticker()
	// Start dedicated applier goroutine
	go rf.applier()

	return rf
}

// Returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currTerm, rf.role == Leader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}

	index := len(rf.logs) + rf.snapshotIndex
	Debug(dInfo, "%v: receive command %+v, index=%v", rf.getIdAndRole(), command, index)
	term := rf.currTerm
	isLeader := true
	rf.logs = append(rf.logs, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.persist()
	rf.matchIndex[rf.me] = index
	go rf.replicateOneRound()

	return index, term, isLeader
}

// The service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotIndex {
		return
	}
	Debug(dSnap, "%v: taking snapshot, index=%v", rf.getIdAndRole(), index)

	snpIdx := rf.snapshotIndex
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.logs[index-snpIdx].Term
	rf.snapshot = snapshot
	rf.logs = CloneLogs(rf.logs[index-snpIdx:])
	rf.persist()
	Debug(dSnap, "%v: truncated log %+v", rf.getIdAndRole(), rf.logs)
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	Debug(dLog, "S%d start running", rf.me)
	for rf.killed() == false {
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			Debug(dTimer, "%v: election timer fires", rf.getIdAndRoleWithLock())
			go rf.startElection()
		}
	}
}

// Save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	Debug(dPersist, "%v: persisted states and snapshot", rf.getIdAndRole())
	rfstates := rf.encodeStates()
	rf.persister.Save(rfstates, rf.snapshot)
}

func (rf *Raft) readPersist() {
	Debug(dPersist, "%v: restored persistent states and snapshot", rf.getIdAndRole())
	rf.decodeStates(rf.persister.ReadRaftState())
	rf.snapshot = rf.persister.ReadSnapshot()
}

func (rf *Raft) encodeStates() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	return w.Bytes()
}

func (rf *Raft) decodeStates(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm, votedFor int
	var logs []LogEntry
	var snapshotIndex, snapshotTerm int
	if d.Decode(&currTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		Debug(dError, "%v: error decoding persistent states", rf.getIdAndRole())
	} else {
		rf.currTerm = currTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}
}

func (rf *Raft) startHeartbeatTimers() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.heartbeatTimers[peer].Reset(HeartbeatInterval * time.Millisecond)
		go func(p int) {
			for !rf.killed() {
				select {
				case <-rf.heartbeatTimers[p].C:
					go rf.sendHeartbeat(p)
				case <-rf.stopHeartbeat:
					rf.heartbeatTimers[p].Stop()
					return
				}
			}
		}(peer)
	}
}

func (rf *Raft) stopHeartbeatTimers() {
	rf.stopHeartbeat <- struct{}{}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(GetRandElectionTimeout())
}

func (rf *Raft) resetHeartbeatTimer(peer int) {
	rf.heartbeatTimers[peer].Stop()
	rf.heartbeatTimers[peer].Reset(HeartbeatInterval * time.Millisecond)
}

func (rf *Raft) lastLogIndexAndTerm() (lastLogIndex int, lastLogTerm int) {
	rawLastLogIndex := len(rf.logs) - 1
	lastLogTerm = rf.logs[rawLastLogIndex].Term
	lastLogIndex = rawLastLogIndex + rf.snapshotIndex
	return
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1 + rf.snapshotIndex
}

func (rf *Raft) convertToCandidate() {
	Debug(dLog, "%v: convert to Candidate", rf.getIdAndRole())
	rf.role = Candidate
	rf.currTerm += 1
	rf.votedFor = rf.me // vote for self
	// Should be persisted shortly after
}

func (rf *Raft) convertToLeader() {
	Debug(dVote, "%v: convert to Leader", rf.getIdAndRole())

	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex() + 1 // Init as lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers)) // Init as 0
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.startHeartbeatTimers()
}

func (rf *Raft) convertToFollower() {
	if rf.role != Follower {
		Debug(dLog, "%v: convert to Follower", rf.getIdAndRole())
	}
	if rf.role == Leader {
		rf.stopHeartbeatTimers()
	}
	rf.role = Follower
}

func (rf *Raft) convertToFollowerWithLock() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.convertToFollower()
}

func (rf *Raft) getIdAndRole() string {
	var role string
	switch rf.role {
	case Leader:
		role = "Leader"
	case Candidate:
		role = "Candidate"
	case Follower:
		role = "Follower"
	}
	return fmt.Sprintf("S%v %v@T%v", rf.me, role, rf.currTerm)
}

func (rf *Raft) getIdAndRoleWithLock() string {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getIdAndRole()
}
