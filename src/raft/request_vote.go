package raft

type RequestVoteArgs struct {
    // Your Data here (2A, 2B).
    Term         int
    CandidateId  int
    LastLogIndex int
    LastLogTerm  int
}

type RequestVoteReply struct {
    // Your Data here (2A).
    Term        int
    VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()

    Debug(dLog, "%s: received RequestVote RPC %+v", rf.getIdAndRole(), *args)
    if args.Term < rf.currTerm {
        reply.Term = rf.currTerm
        reply.VoteGranted = false
        Debug(dVote, "%v: reject vote for candidate %v. Reason: my term higher", rf.getIdAndRole(), args.CandidateId)
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
        Debug(dTimer, "%v: reset election timer after vote for candidate %v", rf.getIdAndRole(), args.CandidateId)
        rf.resetElectionTimer() // Reset election timer only after vote to candidate
        reply.Term = rf.currTerm
        reply.VoteGranted = true
        Debug(dVote, "%v: grant vote for candidate %v", rf.getIdAndRole(), args.CandidateId)
        return
    } else {
        reply.Term = rf.currTerm
        reply.VoteGranted = false
        Debug(dVote, "%v: reject vote for candidate %v. Reason: already voted or candidate log not up-to-date", rf.getIdAndRole(), args.CandidateId)
        Debug(dVote, "%v: candidate last log entry={idx=%v,term=%v}, my last log entry={idx=%v,term=%v}",
            rf.getIdAndRole(), args.LastLogIndex, args.LastLogTerm, rf.lastLogIndex(), rf.logs[rf.lastLogIndex()-rf.snapshotIndex].Term)
        return
    }
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    // No need to implement timeout as Call() always returns
    // given that the RPC handler always return.
    return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) startElection() {
    rf.mu.Lock()
    Debug(dTimer, "%v: reset election timer upon calling startElection()", rf.getIdAndRole())
    rf.resetElectionTimer()
    if rf.role == Leader {
        rf.mu.Unlock()
        return // think I am leader, no need to elect
    }
    rf.convertToCandidate()
    Debug(dVote, "%s: start election for term %d", rf.getIdAndRole(), rf.currTerm)

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
                Debug(dError, "%s: error sending RequestVote RPC to peer %d", rf.getIdAndRoleWithLock(), p)
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

    for !rf.killed() {
        /*
         * This loop will exit if:
         * 1. Collected majority grant or reject.
         * 2. Is not candidate anymore (explored higher term and converted to follower)
         * 3. currTerm changed (e.g.timeout and started new election)
         */
        rf.mu.Lock()
        giveUp := (rf.role != Candidate) || (rf.currTerm != args.Term)
        rf.mu.Unlock()
        if giveUp {
            break
        }

        for granted := range voteCh {
            numVoted += 1
            if granted {
                numGranted += 1
            }

            if numGranted >= n/2+1 {
                // Collected majority grant
                rf.mu.Lock()
                if rf.role == Candidate && rf.currTerm == args.Term {
                    rf.convertToLeader()
                    rf.mu.Unlock()
                    rf.broadcastHeartbeat()
                } else {
                    rf.mu.Unlock()
                }
                return
            } else if numVoted-numGranted >= n/2+1 {
                // Majority rejected, give up
                rf.convertToFollowerWithLock()
                Debug(dTimer, "%v: reset election timer after receiving majority rejection", rf.getIdAndRoleWithLock())
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
