package raft

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("{}: received {:?}", rf.getRoleAndId(), args)
	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		reply.VoteGranted = false
		return
	}

	needPersist := false
	if args.Term > rf.currTerm {
		needPersist = true
		rf.currTerm = args.Term
		rf.convertToFollower()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.compareLog(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer() // Reset election timer after vote to candidate
		reply.Term = rf.currTerm
		reply.VoteGranted = true
		return
	} else {
		if needPersist {
			rf.persist()
		}
		reply.Term = rf.currTerm
		reply.VoteGranted = false
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {

}

// compareLog returns true if given (lastLogIndex, lastLogTerm) is
// *at least up-to-date* as my log. Otherwise, returns false.
func (rf *Raft) compareLog(lastLogIndex, lastLogTerm int) bool {
	myLastLogIndex, myLastLogTerm := rf.lastLogIndexAndTerm()
	if lastLogTerm > myLastLogTerm {
		return true
	} else if lastLogTerm < myLastLogTerm {
		return false
	} else {
		return lastLogIndex >= myLastLogIndex
	}
}
