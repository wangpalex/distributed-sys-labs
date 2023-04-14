package raft

import "sort"

type AppendEntriesArgs struct {
    // Your Data here (2A, 2B).
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    // Your Data here (2A).
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
    defer Debug(dTimer, "%v: reset election timer after receiving AE RPC", rf.getIdAndRole())
    Debug(dLog, "%s: received AppendEntries RPC", rf.getIdAndRole())
    if len(args.Entries) > 0 {
        Debug(dLog, "%v: RPC entries first=%+v, last=%+v", rf.getIdAndRole(), args.Entries[0], args.Entries[len(args.Entries)-1])
    }

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

    snpIdx := rf.snapshotIndex
    if args.PrevLogIndex < snpIdx {
        // First entry is included in snapshot, ask leader to align and try again.
        Debug(dTrace, "%v: prevIndex=%v < snpIdx=%v, ask leader align nextIdx and retry", rf.getIdAndRole(), args.PrevLogIndex, snpIdx, rf.snapshotIndex+1)
        reply.Term = rf.currTerm
        reply.Success = false
        reply.NextIndex = snpIdx + 1
        return
    }

    if args.PrevLogIndex > rf.lastLogIndex() || rf.logs[args.PrevLogIndex-snpIdx].Term != args.PrevLogTerm {
        // Logs do not have matching prev log entry
        Debug(dTrace, "%v: do not have matching prevLogEntry", rf.getIdAndRole())
        reply.Term = rf.currTerm
        reply.Success = false
        reply.NextIndex = rf.lastMatchingIndex(min(args.PrevLogIndex, rf.lastLogIndex()), args.PrevLogTerm) + 1
        return
    }

    for i, entry := range args.Entries {
        idx := args.PrevLogIndex + 1 + i
        if idx > rf.lastLogIndex() || rf.logs[idx-snpIdx].Term != entry.Term {
            // Replace all subsequent entries starting at un-matching entry
            rf.logs = rf.logs[:idx-snpIdx]
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

    if rf.nextIndex[peer] <= rf.snapshotIndex {
        rf.mu.Unlock()
        go rf.sendSnapshot(peer)
        return
    }

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
    snpIdx := rf.snapshotIndex
    // Debug(dTrace, "%v: trying to get entries to send to peer %v, nextIndex=%v, snapshotIndex=%v,log=%+v", rf.getIdAndRole(), peer, nextIndex, snpIdx, rf.logs)
    prevLogIndex = nextIndex - 1
    prevLogTerm = rf.logs[prevLogIndex-snpIdx].Term
    entries = append(entries, rf.logs[nextIndex-snpIdx:]...)
    return
}

func (rf *Raft) lastMatchingIndex(prevLogIndex, prevLogTerm int) int {
    i := prevLogIndex
    snpIdx := rf.snapshotIndex
    for i > snpIdx && rf.logs[i-snpIdx].Term != prevLogTerm {
        i -= 1
    }
    return i
}

func (rf *Raft) applyLogs() {
    rf.mu.Lock()
    if rf.lastApplied < rf.snapshotIndex ||
        rf.commitIndex <= rf.lastApplied {
        Debug(dTrace, "%v: did not apply logs, lastApplied=%v, snapshotIndex=%v, commitIndex=%v", rf.getIdAndRole(), rf.lastApplied, rf.snapshotIndex, rf.commitIndex)
        rf.mu.Unlock()
        return
    }
    snpIdx := rf.snapshotIndex
    baseIdx := rf.lastApplied + 1
    applyEntries := make([]LogEntry, 0, rf.commitIndex-rf.lastApplied)
    applyEntries = append(applyEntries, rf.logs[rf.lastApplied+1-snpIdx:rf.commitIndex+1-snpIdx]...)
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
    snpIdx := rf.snapshotIndex
    matchIndex := make([]int, 0, n)
    matchIndex = append(matchIndex, rf.matchIndex...)
    sort.Ints(matchIndex)
    Debug(dLog, "%v: trying to commit, matchIndex=%+v", rf.getIdAndRole(), matchIndex)
    // Find median to be commit index -> replicated on majority
    median := matchIndex[n/2]
    /*
     * Must check index term matches currTerm.
     * An entry is committed ONLY IF (it's replicated
     * on majority AND it matches leader's term).
     * Refer to Figure 8 of raft-extended paper.
     */
    okToCommit := median > rf.commitIndex && rf.logs[median-snpIdx].Term == rf.currTerm
    if okToCommit {
        rf.commitIndex = median
        Debug(dCommit, "%v: set commit index to %v", rf.getIdAndRole(), rf.commitIndex)
        go rf.applyLogs()
    }
}
