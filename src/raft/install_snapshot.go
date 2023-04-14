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
	Debug(dSnap, "%v: received InstallSnapshot RPC, snapshot(index=%v, term=%v)", rf.getIdAndRole(), args.SnapshotIndex, args.SnapshotTerm)

	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		return
	}

	if rf.snapshotIndex < args.SnapshotIndex {
		rf.snapshot = args.Data
		rf.snapshotIndex = args.SnapshotIndex
		rf.snapshotTerm = args.SnapshotTerm

		if rf.logs[len(rf.logs)-1].Index < args.SnapshotIndex {
			// No existing entry match snapshot index
			// Discard log and use snapshot as first log entry
			rf.logs = rf.logs[len(rf.logs)-1:]
		} else {
			// Truncate entries before snapshot index
			rf.discardLogsBefore(args.SnapshotIndex)
		}
		// Rewrite logs[0] to match snapshot
		rf.logs[0] = LogEntry{Index: args.SnapshotIndex, Term: args.SnapshotTerm}
		Debug(dSnap, "%v: set snapshot, index=%v, logs %+v", rf.getIdAndRole(), rf.snapshotIndex, rf.logs)
		rf.commitIndex = max(rf.commitIndex, rf.snapshotIndex)
		if rf.lastApplied < rf.snapshotIndex {
			go rf.applySnapshot()
		}
		rf.persist()
	} else {
		Debug(dSnap, "%v: ignored snapshot, my snpIdx=%v, snpTerm=%v, logs %+v", rf.getIdAndRole(), rf.snapshotIndex, rf.snapshotTerm, rf.logs)
	}

	reply.Term = rf.currTerm
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) sendSnapshot(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	Debug(dSnap, "%v: sending snapshot to peer %v", rf.getIdAndRole(), peer)

	// Simplification: send snapshot always in one chunk
	dataChunk := make([]byte, 0, len(rf.snapshot))
	dataChunk = append(dataChunk, rf.snapshot...)
	args := InstallSnapshotArgs{
		Term:          rf.currTerm,
		LeaderId:      rf.me,
		SnapshotIndex: rf.snapshotIndex,
		SnapshotTerm:  rf.snapshotTerm,
		Data:          dataChunk,
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	if !rf.sendInstallSnapshot(peer, &args, &reply) {
		Debug(dError, "%v: error sending snapshot to peer %v", rf.getIdAndRoleWithLock(), peer)
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
	rf.logs = rf.logs[from:]
}

func (rf *Raft) applySnapshot() {
	rf.mu.Lock()
	data := make([]byte, 0, len(rf.snapshot))
	data = append(data, rf.snapshot...)
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  rf.snapshotTerm,
		SnapshotIndex: rf.snapshotIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg
	rf.mu.Lock()
	rf.lastApplied = msg.SnapshotIndex
	Debug(dLog2, "%v: applied snapshot, index=%v", rf.getIdAndRole(), msg.SnapshotIndex)
	rf.mu.Unlock()
}
