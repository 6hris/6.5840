package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//3A
	currentTerm     int
	votedFor        int
	log             []*LogEntry
	state           RaftState
	lastContact     time.Time
	electionTimeout time.Duration
	//3B
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan raftapi.ApplyMsg
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	// !! Lock must be held to call this function !!
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []*LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// 3C
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = len(rf.log)

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.becomeFollowerLocked(args.Term)
	reply.Term = rf.currentTerm

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		x := args.PrevLogIndex
		for x > 0 && rf.log[x-1].Term == reply.XTerm {
			x--
		}
		reply.XIndex = x
		reply.Success = false
		return
	}

	insertPos := args.PrevLogIndex + 1
	i := 0

	for insertPos+i < len(rf.log) && i < len(args.Entries) {
		if rf.log[insertPos+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:insertPos+i]
			break
		}
		i++
	}

	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Success = true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	receiverLastLogIndex := len(rf.log) - 1
	receiverLastLogTerm := rf.log[receiverLastLogIndex].Term
	isUpToDate := false

	if args.LastLogTerm > receiverLastLogTerm ||
		(args.LastLogTerm == receiverLastLogTerm && args.LastLogIndex >= receiverLastLogIndex) {
		isUpToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimerLocked()
		rf.persist()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func getRandomTimeDuration() time.Duration {
	return time.Duration(500+(rand.Int63()%400)) * time.Millisecond
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.lastContact = time.Now()
	rf.electionTimeout = getRandomTimeDuration()
}

func (rf *Raft) becomeFollowerLocked(newTerm int) {
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.resetElectionTimerLocked()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	termStarted := rf.currentTerm
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.lastContact = time.Now()
	rf.electionTimeout = getRandomTimeDuration() // 150-300ms
	votes := 1

	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term

	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Candidate || rf.currentTerm != termStarted {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}

			if reply.VoteGranted {
				votes++
				if votes > (len(rf.peers) / 2) {
					rf.state = Leader
					for peer := range rf.peers {
						rf.nextIndex[peer] = len(rf.log)
						if peer == rf.me {
							rf.matchIndex[peer] = len(rf.log) - 1
						} else {
							rf.matchIndex[peer] = 0
						}
					}
				}
			}
		}(peer)
	}

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

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		term = rf.currentTerm
		return index, rf.currentTerm, isLeader
	}
	entry := &LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, entry)
	rf.persist()
	term = rf.currentTerm
	index = len(rf.log) - 1

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
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(30 * time.Millisecond)

		rf.mu.Lock()
		state := rf.state
		isTimedOut := time.Since(rf.lastContact) > rf.electionTimeout
		rf.mu.Unlock()
		if state != Leader && isTimedOut {
			rf.startElection()
		}
	}
}

func (rf *Raft) heartbeatLoop() {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond) // hearbeat interval
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state == Leader {
			// send heartbeat to each server
			for peer := range rf.peers {
				if peer == rf.me {
					continue
				}
				go rf.sendToPeer(peer)
			}
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) sendToPeer(peer int) {
	rf.mu.Lock()
	var reply AppendEntriesReply
	args := &AppendEntriesArgs{}
	args.LeaderId = rf.me
	args.Term = rf.currentTerm

	start := rf.nextIndex[peer]
	entries := make([]*LogEntry, len(rf.log[start:]))
	copy(entries, rf.log[start:])
	args.Entries = entries

	args.LeaderCommit = rf.commitIndex
	args.PrevLogIndex = start - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}

	if reply.Success {
		progress := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[peer] = progress
		rf.nextIndex[peer] = progress + 1
		rf.matchIndex[rf.me] = progress
		rf.nextIndex[rf.me] = progress + 1
		rf.advanceCommitIndexLocked()
	} else {
		if reply.XTerm == -1 {
			rf.nextIndex[peer] = reply.XLen
		} else {
			last := rf.findLastIndexOfTermLocked(reply.XTerm)
			if last != -1 {
				rf.nextIndex[peer] = last + 1
			} else {
				rf.nextIndex[peer] = reply.XIndex
			}
		}
	}

}

func (rf *Raft) findLastIndexOfTermLocked(term int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			return i
		}
	}
	return -1
}

func (rf *Raft) advanceCommitIndexLocked() {
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > (len(rf.peers) / 2) {
			rf.commitIndex = N
			return
		}
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = -1
	rf.log = []*LogEntry{{Term: 0}}
	rf.electionTimeout = getRandomTimeDuration()
	rf.lastContact = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatLoop()
	go rf.applier()

	return rf
}
