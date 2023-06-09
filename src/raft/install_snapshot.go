package raft

type InstallSnapshotArgs struct {
	Term          int
	LeaderId      int
	SnapshotIndex int    // Last included index of snapshot.
	SnapshotTerm  int    // Last included entry term of snapshot.
	Data          []byte // Raw bytes of snapshot Data chunk.
	Offset        int    // Offset of the Data chunk
	Done          bool   // True if is the last chunk
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "%v: received InstallSnapshot RPC, snapshot{index=%v, term=%v}", rf.getIdAndRole(), args.SnapshotIndex, args.SnapshotTerm)

	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		return
	}
	// The leader is not stale
	Debug(dTimer, "%v: reset election timer after receiving InstallSnapshot from S%v Leader@T%v", rf.getIdAndRole(), args.LeaderId, args.Term)
	rf.resetElectionTimer()

	reply.Term = rf.currTerm
	if rf.snapshotIndex >= args.SnapshotIndex {
		Debug(dSnap, "%v: ignored install-snapshot, my snpIdx=%v, logs %+v", rf.getIdAndRole(), rf.snapshotIndex, rf.logs)
		return
	}

	rf.snapshot = args.Data
	rf.snapshotIndex = args.SnapshotIndex
	rf.snapshotTerm = args.SnapshotTerm
	if rf.logs[len(rf.logs)-1].Index < args.SnapshotIndex {
		// No existing entry match snapshot index
		// Discard logs and use snapshot as first log entry
		rf.logs = make([]LogEntry, 1)
	} else {
		// Discard entries before snapshot index
		rf.discardLogsBefore(args.SnapshotIndex)
	}
	// Rewrite logs[0] to match snapshot
	rf.logs[0] = LogEntry{Index: args.SnapshotIndex, Term: args.SnapshotTerm}
	Debug(dSnap, "%v: set snapshot, index=%v, logs %+v", rf.getIdAndRole(), rf.snapshotIndex, rf.logs)
	rf.commitIndex = Max(rf.commitIndex, rf.snapshotIndex)
	if rf.lastApplied < rf.snapshotIndex {
		go rf.applySnapshot()
	}
	rf.persist()
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) sendSnapshot(peer int) {
	rf.mu.RLock()
	if rf.role != Leader {
		rf.mu.RUnlock()
		return
	}
	idAndRole := rf.getIdAndRole()
	Debug(dSnap, "%v: sending snapshot{index=%v, term=%v} to peer %v", idAndRole, rf.snapshotIndex, rf.snapshotTerm, peer)

	// Simplification: send snapshot always in one chunk
	data := CloneBytes(rf.snapshot)
	args := InstallSnapshotArgs{
		Term:          rf.currTerm,
		LeaderId:      rf.me,
		SnapshotIndex: rf.snapshotIndex,
		SnapshotTerm:  rf.snapshotTerm,
		Data:          data,
	}
	rf.mu.RUnlock()

	reply := InstallSnapshotReply{}
	if !rf.sendInstallSnapshot(peer, &args, &reply) {
		Debug(dError, "%v: error sending snapshot to peer %v", idAndRole, peer)
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

	rf.nextIndex[peer] = args.SnapshotIndex + 1
	rf.matchIndex[peer] = args.SnapshotIndex
	go rf.commitLogs()
}

func (rf *Raft) discardLogsBefore(index int) {
	var from int
	for i, entry := range rf.logs {
		if entry.Index == index {
			from = i
			break
		}
	}
	rf.logs = CloneLogs(rf.logs[from:])
}

func (rf *Raft) applySnapshot() {
	rf.mu.RLock()
	data := CloneBytes(rf.snapshot)
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  rf.snapshotTerm,
		SnapshotIndex: rf.snapshotIndex,
	}
	rf.mu.RUnlock()

	rf.applyCh <- msg
	rf.mu.Lock()
	rf.lastApplied = msg.SnapshotIndex
	Debug(dLog2, "%v: applied snapshot{index=%v, term=%v}", rf.getIdAndRole(), msg.SnapshotIndex, msg.SnapshotTerm)
	rf.mu.Unlock()
}
