package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

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

	DPrintf("%s: received RequestVote RPC %+v", rf.getRoleAndId(), *args)
	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		reply.VoteGranted = false
		DPrintf("%v: reject vote for candidate %v. Reason: my term greater", rf.getRoleAndId(), args.CandidateId)
		return
	}

	if args.Term > rf.currTerm {
		rf.currTerm = args.Term
		rf.votedFor = -1
		rf.convertToFollower()
		rf.persist()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.compareLog(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer() // Reset election timer only after vote to candidate
		reply.Term = rf.currTerm
		reply.VoteGranted = true
		DPrintf("%v: grant vote for candidate %v", rf.getRoleAndId(), args.CandidateId)
		return
	} else {
		reply.Term = rf.currTerm
		reply.VoteGranted = false
		DPrintf("%v: reject vote for candidate %v. Reason: already voted or log not up-to-date", rf.getRoleAndId(), args.CandidateId)
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// No need to implement timeout as Call() always returns
	// given that the RPC handler always return.
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) startElection() {
	if rf.role == Leader {
		return // think I am leader, no need to elect
	}

	rf.mu.Lock()
	rf.convertToCandidate()
	rf.resetElectionTimer()
	DPrintf("%s: start election for term %d", rf.getRoleAndId(), rf.currTerm)

	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	args := RequestVoteArgs{
		Term:         rf.currTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	n := len(rf.peers)
	numVoted := 1
	numGranted := 1
	voteCh := make(chan bool)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(p, &args, &reply) {
				DPrintf("%s: error sending RequestVote RPC to peer %d", rf.getRoleAndIdWithLock(), p)
				return
			}
			// Handle reply
			rf.mu.Lock()
			if reply.Term > rf.currTerm {
				rf.currTerm = reply.Term
				rf.votedFor = -1
				rf.convertToFollower()
				rf.persist()
			}
			rf.mu.Unlock()
			voteCh <- reply.VoteGranted
		}(peer)
	}

	for {
		/*
		 * This loop will exit if:
		 * 1. Collected majority grant or reject.
		 * 2. Is not candidate anymore (explored higher term and convert to follower)
		 * 3. currTerm changed (e.g.timeout and started new election)
		 */
		rf.mu.Lock()
		giveUp := (rf.role != Candidate) || (rf.currTerm != args.Term)
		rf.mu.Unlock()
		if giveUp {
			break
		}

		select {
		case granted := <-voteCh:
			numVoted += 1
			if granted {
				numGranted += 1
			}

			if numGranted >= n/2+1 {
				// Collected majority grant
				if rf.role == Candidate && rf.currTerm == args.Term {
					rf.convertToLeader()
					rf.broadcastHeartbeat()
				}
				return
			} else if numVoted-numGranted >= n/2+1 {
				// Majority rejected, give up
				rf.convertToFollowerWithLock()
				rf.resetElectionTimer()
				return
			}
		}
	}
}

// compareLog returns true if given (lastLogIndex, lastLogTerm) is
// at least up-to-date as my log. Otherwise, returns false.
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
