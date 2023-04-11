package raft

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term      int
	Success   bool
	NextIndex int // Suggested next index for leader to sync
}

// RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.resetElectionTimer()

	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currTerm {
		rf.currTerm = args.Term
		rf.votedFor = -1
		rf.convertToFollower()
		rf.persist()
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currTerm
		reply.Success = false
		reply.NextIndex = rf.lastMatchingIndex(args.PrevLogIndex, args.PrevLogTerm) + 1
		return
	}

	i := 0
	offset := args.PrevLogIndex + 1
	for i < len(args.Entries) {
		if rf.logs[offset+i].Term != args.Entries[i].Term {
			copy(rf.logs[offset+i:], args.Entries[i:])
			break
		}
	}
	reply.Term = rf.currTerm
	reply.Success = true
	reply.NextIndex = args.PrevLogIndex + 1 + len(args.Entries)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(peer int) {
	// To be completed 2B
	if rf.role != Leader {
		return
	}

	rf.heartbeatTimers[peer].Stop()
	DPrintf("%s: sending AppendEntries RPC", rf.getRoleAndId())

	rf.mu.Lock()
	prevLogIndex, prevLogTerm, entries := rf.getEntriesToSend(peer)
	args := AppendEntriesArgs{
		Term:         rf.currTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if !rf.sendAppendEntries(peer, &args, &reply) {
		DPrintf("%s: error sending AppendEntries RPC to peer %d", rf.getRoleAndId(), peer)
		return
	}

	// Handle reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currTerm {
		rf.currTerm = reply.Term
		rf.votedFor = -1
		rf.convertToFollower()
		rf.persist()
		return
	}

	if !reply.Success {
		rf.nextIndex[peer] = reply.NextIndex
	} else {
		rf.nextIndex[peer] = reply.NextIndex
		rf.matchIndex[peer] = reply.NextIndex - 1
		rf.tryCommitLog()
	}

	rf.resetHeartbeatTimer(peer)
}

func (rf *Raft) broadcastHeartbeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendHeartbeat(peer)
	}
}

func (rf *Raft) getEntriesToSend(peer int) (prevLogIndex, prevLogTerm int, entries []LogEntry) {
	nextIndex := rf.nextIndex[peer]
	prevLogIndex = nextIndex - 1
	prevLogTerm = rf.logs[prevLogIndex].Term
	copy(entries, rf.logs[nextIndex:])
	return
}

func (rf *Raft) lastMatchingIndex(prevLogIndex, prevLogTerm int) int {
	i := prevLogIndex
	for i > 0 && rf.logs[i].Term != prevLogTerm {
		i -= 1
	}
	return i
}

func (rf *Raft) tryCommitLog() {

}
