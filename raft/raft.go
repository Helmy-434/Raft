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
	"lab2raft/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // Reserved for a later lab; ignore in Lab 2.
	Snapshot    []byte // Reserved for a later lab; ignore in Lab 2.
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// TO BE REPLACED BY HELMY
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// TO BE REPLACED BY HELMY

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg
	dead    int32 // for Kill()

	state       State
	currentTerm int
	votedFor    int        // -1 = none
	log         []LogEntry // log[0] is a dummy entry, so real indexes start at 1

	commitIndex int
	lastApplied int
	nextIndex   []int // leader only
	matchIndex  []int // leader only

	electionTimeout time.Duration
	lastHeard       time.Time  // election timer
	applyCond       *sync.Cond // wakes the apply goroutine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // voter's currentTerm, so the candidate can step down
	VoteGranted bool // true = vote given
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// stale candidate
	if args.Term < rf.currentTerm {
		return
	}

	// newer term: adopt it (this also clears votedFor)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	// election restriction: candidate's log must be at least as up to date
	myLastTerm := rf.lastLogTerm()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= rf.lastLogIndex())

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer() // only reset when we actually grant the vote
		DPrintf("[Peer %d] Voted for %d in term %d", rf.me, args.CandidateId, rf.currentTerm)
	}
}

func (rf *Raft) lastLogIndex() int { return len(rf.log) - 1 }
func (rf *Raft) lastLogTerm() int  { return rf.log[len(rf.log)-1].Term }

// random timeout in [350ms, 650ms)
func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
	rf.electionTimeout = time.Duration(350+rand.Intn(300)) * time.Millisecond
}

// Step down. Only a HIGHER term clears votedFor.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1 // optimistic guess
		rf.matchIndex[i] = 0                    // nothing confirmed yet
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	DPrintf("[Peer %d] Became LEADER for term %d", rf.me, rf.currentTerm)
	go rf.HeartbeatLoop(rf.currentTerm) // sends the first heartbeat immediately
}

// background goroutine: starts an election when the timer expires
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.lastHeard) >= rf.electionTimeout {
			rf.startElection() // called with the lock held
		}
		rf.mu.Unlock()
	}
}

// caller must hold rf.mu
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer() // if this election splits, the timeout starts a new one
	DPrintf("[Peer %d] Starting election for term %d", rf.me, rf.currentTerm)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}

	votes := 1 // my own vote; only modified while holding rf.mu

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(peer, args, reply) { // no lock held during the RPC
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// ignore stale replies
			if rf.state != Candidate || rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeader()
				}
			}
		}(i)
	}
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int        // leader's term
	LeaderId     int        // so followers know who the leader is
	PrevLogIndex int        // index of the entry just before the new ones
	PrevLogTerm  int        // term of that entry
	Entries      []LogEntry // entries to store (empty for a heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	// Your data here (3A, 3B).
	Term          int  // follower's currentTerm, so a stale leader can step down
	Success       bool // true if the follower had PrevLogIndex/PrevLogTerm and accepted
	ConflictIndex int  // hint: where the leader should retry from
	ConflictTerm  int  // hint: term of the conflicting entry, or -1 if the log was too short
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	if args.Term < rf.currentTerm { // old leader sending stuff
		reply.Term = rf.currentTerm
		DPrintf("[Peer %d] Reject AppendEntries from %d, reason: args.Term %d < peers curr term %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm { // higher term so become a follower
		rf.becomeFollower(args.Term)
		DPrintf("[Peer %d] Found a higher term %d from %d, becoming Follower", rf.me, args.Term, args.LeaderId)
	} else if rf.state != Follower { // if it was a candidate and lost
		rf.state = Follower
		DPrintf("[Peer %d] Received AppendEntries in same term %d from %d while %v, becoming Follower", rf.me, args.Term, args.LeaderId, rf.state)
	}
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm
	// DPrintf("[Peer %d] Resetting Election Timer",rf.me)

	// identifying where are the conflicts if they exist
	if args.PrevLogIndex >= len(rf.log) { // follower was offline and missed some entries
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log)
		DPrintf("[Peer %d] Reject AppendEntries from %d, reason: log too short (len=%d, prevLogIndex=%d)", rf.me, args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // conflicting terms
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		i := args.PrevLogIndex
		for i > 1 && rf.log[i-1].Term == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		DPrintf("[Peer %d] Reject AppendEntries from %d, reason: term mismatch at %d (got %d, want %d), conflictIndex=%d", rf.me, args.LeaderId, args.PrevLogIndex, reply.ConflictTerm, args.PrevLogTerm, reply.ConflictIndex)
		return
	}

	// conflict resolution
	for i, entry := range args.Entries {
		logIdx := args.PrevLogIndex + i + 1
		if logIdx < len(rf.log) {
			if rf.log[logIdx].Term != entry.Term {
				DPrintf("[Peer %d] Conflict at index %d (local term %d != leader term %d), truncating log and appending %d entries", rf.me, logIdx, rf.log[logIdx].Term, entry.Term, len(args.Entries[i:]))
				rf.log = rf.log[:logIdx]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			DPrintf("[Peer %d] Appending %d new entries starting at index %d", rf.me, len(args.Entries[i:]), logIdx)
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	// advance commit idx and wakes applyLoop()
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		newCommit := args.LeaderCommit
		if lastNewIndex < newCommit {
			newCommit = lastNewIndex
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			DPrintf("[Peer %d] Updated commitIndex to %d", rf.me, rf.commitIndex)
			rf.applyCond.Broadcast()
		}
	}
	reply.Success = true

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()

	// if the server is not the leader or shutting down; then dont change the log
	if rf.state != Leader || rf.killed() {
		term = rf.currentTerm
		rf.mu.Unlock()
		return index, term, isLeader
	}

	term = rf.currentTerm
	isLeader = true

	// get the index of the new log entry
	index = len(rf.log)
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	DPrintf("Leader %d added log entry %d with command %v in term %d", rf.me, index, command, term)

	rf.mu.Unlock()
	rf.BroadcastAppendEntries()

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast() // wake applyLoop so it can exit
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		start := rf.lastApplied + 1
		end := rf.commitIndex
		msgs := make([]ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			msgs = append(msgs, ApplyMsg{Index: i, Command: rf.log[i].Command})
		}
		rf.lastApplied = end
		DPrintf("[Peer %d] Batching entries %d..%d for applyCh", rf.me, start, end)
		rf.mu.Unlock()

		for _, msg := range msgs {
			if rf.killed() {
				return
			}
			DPrintf("[Peer %d] Applying index=%d command=%v", rf.me, msg.Index, msg.Command)
			rf.applyCh <- msg
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.applyCh = applyCh

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}} // dummy entry at index 0

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()

	go rf.ticker()
	go rf.applyLoop()

	return rf
}

func (rf *Raft) HeartbeatLoop(term int) {
	for !rf.killed() {

		if t, isLeader := rf.GetState(); !isLeader || t != term {
			return
		}

		rf.BroadcastAppendEntries()
		time.Sleep(110 * time.Millisecond) // send heartbeat every 110ms
	}
}

func (rf *Raft) BroadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.ReplicateTo(i)
	}
}

func (rf *Raft) ReplicateTo(peer int) {
	//bulid args under lock
	rf.mu.Lock()
	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	next := rf.nextIndex[peer]
	if next < 1 {
		next = 1
	}

	if next > len(rf.log) {
		next = len(rf.log)
	}

	prevIndex := next - 1
	entries := make([]LogEntry, len(rf.log)-next)
	copy(entries, rf.log[next:])

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.log[prevIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	//send with no lock
	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(peer, args, reply) {
		return
	}

	//handle reply under lock, re-validating everything
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != args.Term {
		return // stepped down or term changed while we were waiting
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] { // never move backwards
			rf.matchIndex[peer] = newMatch
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.AdvanceCommitIndex()
		return
	}

	// Failure: ignore if stale (a newer request already changed nextIndex)
	if rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}

	newNext := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		// Does the leader have an entry with the follower's conflicting term?
		for i := len(rf.log) - 1; i >= 1; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				newNext = i + 1 // resume after our last entry of that term
				break
			}
		}
	}
	if newNext < 1 {
		newNext = 1
	}
	if newNext > len(rf.log) {
		newNext = len(rf.log)
	}
	rf.nextIndex[peer] = newNext

	DPrintf("leader %d sending AppendEntries to %d, nextIndex -> %d", rf.me, peer, newNext)

	// Retry right away instead of waiting ~110ms for the next heartbeat.
	go rf.ReplicateTo(peer)
}

func (rf *Raft) AdvanceCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			break // only advance commitIndex for entries in the current term
		}

		count := 1 // leader itself
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			DPrintf("Leader %d advanced commitIndex to %d", rf.me, rf.commitIndex)
			rf.applyCond.Broadcast() // wake up applier goroutine
			break
		}
	}
}
