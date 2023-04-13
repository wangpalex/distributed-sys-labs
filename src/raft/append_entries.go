package raft

import "sort"

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term      int
	Success   bool
	NextIndex int // Suggested next index for leader
}

// RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.resetElectionTimer()
	Debug(dLog, "%s: received AppendEntries RPC %+v", rf.getIdAndRole(), *args)

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

	if args.PrevLogIndex > rf.lastLogIndex() || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Do not have matching prev log entry
		Debug(dLog, "%v: do not have matching prevLogEntry", rf.getIdAndRole())
		reply.Term = rf.currTerm
		reply.Success = false
		reply.NextIndex = rf.lastMatchingIndex(min(args.PrevLogIndex, rf.lastLogIndex()), args.PrevLogTerm) + 1
		return
	}

	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx > rf.lastLogIndex() || rf.logs[idx].Term != entry.Term {
			// Replace all subsequent entries starting at un-matching entry
			rf.logs = rf.logs[:idx]
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	lastNewIndex := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
		go rf.applyLogs()
	}
	reply.Term = rf.currTerm
	reply.Success = true
	reply.NextIndex = lastNewIndex + 1
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendHeartbeat(peer int) {
	// To be completed 2B
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	rf.resetHeartbeatTimer(peer)

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
	Debug(dLeader, "%s: sending AppendEntries RPC to peer %v", rf.getIdAndRoleWithLock(), peer)
	if !rf.sendAppendEntries(peer, &args, &reply) {
		Debug(dError, "%s: error sending AppendEntries RPC to peer %d", rf.getIdAndRoleWithLock(), peer)
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
		if len(args.Entries) > 0 {
			go rf.commitLogs()
		}
	}
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
	entries = append(entries, rf.logs[nextIndex:]...)
	return
}

func (rf *Raft) lastMatchingIndex(prevLogIndex, prevLogTerm int) int {
	i := prevLogIndex
	for i > 0 && rf.logs[i].Term != prevLogTerm {
		i -= 1
	}
	return i
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	if rf.commitIndex < rf.lastApplied {
		rf.mu.Unlock()
		return
	}
	baseIdx := rf.lastApplied + 1
	applyEntries := make([]LogEntry, 0, rf.commitIndex-rf.lastApplied)
	applyEntries = append(applyEntries, rf.logs[rf.lastApplied+1:rf.commitIndex+1]...)
	rf.mu.Unlock()

	for i, entry := range applyEntries {
		idx := i + baseIdx
		cmd := entry.Command
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: idx,
		}
		rf.mu.Lock()
		rf.lastApplied = idx
		Debug(dLog2, "%v: applied log index=%v, command=%v", rf.getIdAndRole(), idx, cmd)
		rf.mu.Unlock()
	}
}

func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	n := len(rf.peers)
	matchIndex := make([]int, 0, n)
	matchIndex = append(matchIndex, rf.matchIndex...)
	sort.Ints(matchIndex)
	Debug(dLog, "%v: trying to commit, matchIndex=%+v", rf.getIdAndRole(), matchIndex)
	// Find median to be commit index -> replicated on majority
	newCommitIndex := matchIndex[n/2]
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		Debug(dCommit, "%v: commit index set to %v", rf.getIdAndRole(), rf.commitIndex)
		go rf.applyLogs()
	}
}
