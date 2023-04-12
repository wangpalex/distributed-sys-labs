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
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	// "6.5840/labgob"
	"6.5840/labrpc"
)

const (
	// In milliseconds
	ElectionTimeout   = 1000
	HeartbeatInterval = 100
)

const (
	Leader = iota
	Candidate
	Follower
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states
	currTerm int
	votedFor int
	logs     []LogEntry

	// Volatile states
	role        int
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// channels
	electionTimer   *time.Timer
	heartbeatTimers []*time.Timer
	stopHeartbeat   chan struct{}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1) // Log starts at index 1
	rf.role = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer = time.NewTimer(GetInitElectionTimeout())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	DPrintf("Peer %d started", rf.me)
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			go rf.startElection()
		}
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) startHeartbeatTimers() {
	rf.heartbeatTimers = make([]*time.Timer, len(rf.peers))
	rf.stopHeartbeat = make(chan struct{})
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.heartbeatTimers[peer] = time.NewTimer(HeartbeatInterval)
		go func(p int) {
			for !rf.killed() {
				select {
				case <-rf.heartbeatTimers[p].C:
					go rf.sendHeartbeat(p)
				case <-rf.stopHeartbeat:
					return
				}
			}
		}(peer)
	}
}

func (rf *Raft) stopHeartbeatTimers() {
	close(rf.stopHeartbeat)
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
	lastLogIndex = len(rf.logs) - 1
	lastLogTerm = rf.logs[lastLogIndex].Term
	return
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) convertToCandidate() {
	DPrintf("%s -> Candidate", rf.getRoleAndId())
	rf.role = Candidate
	rf.currTerm += 1
	rf.votedFor = rf.me // vote for self
}

func (rf *Raft) convertToLeader() {
	DPrintf("%s -> Leader", rf.getRoleAndId())
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs) // Init as lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers)) // All init as 0
	rf.startHeartbeatTimers()
}

func (rf *Raft) convertToFollower() {
	if rf.role != Follower {
		DPrintf("%s -> Follower", rf.getRoleAndId())
	}
	if rf.role == Leader {
		rf.stopHeartbeatTimers()
	}
	rf.role = Follower
}

func (rf *Raft) getRoleAndId() string {
	var role string
	switch rf.role {
	case Leader:
		role = "Leader"
	case Candidate:
		role = "Candidate"
	case Follower:
		role = "Follower"
	}
	return fmt.Sprintf("%s %d", role, rf.me)
}
