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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Reporting to service/tester
	applyCh chan ApplyMsg

	// Persistent
	currentTerm int
	log         []Log

	votedFor int // index into peers[] ; -1 is null value
	leader   int // index into peers[] of last known leader

	// Timer
	timer *time.Timer

	// Volatile
	// commitIndex int
	// lastApplied int

	// as a Leader (must be reinitialised each term)
	// nextIndex  []int
	// matchIndex []int
}
type Log struct {
	term  int
	value interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	// Your code here.
	term = rf.currentTerm
	isLeader = rf.leader == rf.me
	return
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	Leader      int
	Sender      int
}

type AppendEntryArgs struct {
	Term         int
	Leader       int
	Value        interface{}
	PrevLogTerm  int
	PrevLogIndex int
}

type AppendEntryReply struct {
	Term     int
	Accepted bool
	Leader   int
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	reply.Term = args.Term

	// Reject old leader
	if rf.currentTerm > args.Term {
		reply.Accepted = false
		reply.Term = rf.currentTerm
		reply.Leader = rf.leader
		return
	}

	rf.leader = args.Term
	rf.restartTimer()

	// Heartbeat check
	if args.Value == nil {
		reply.Accepted = true
		return
	}

	// Checking if previous entry in my log matches previous entry of incoming log
	L := len(rf.log)
	if L > 0 &&
		(rf.log[L-1].term != args.PrevLogTerm ||
			L-1 != args.PrevLogIndex) {
		reply.Accepted = false
		return
	}

	// Else, accept incoming request
	rf.log = append(rf.log, Log{term: args.Term, value: args.Value})
	reply.Accepted = true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = args.Term
	reply.Sender = rf.me
	vf := rf.votedFor

	// Simply reject old election?
	// if rf.currentTerm > args.Term {
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if vf == args.CandidateId {
		reply.VoteGranted = true
		// reply.Term = -1
		return
	}

	if vf != -1 {
		reply.VoteGranted = false
		return
	}

	L := len(rf.log)
	if L > 0 &&
		(rf.log[len(rf.log)-1].term > args.LastLogTerm ||
			len(rf.log)-1 > args.LastLogIndex) {

		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	reply.Term = args.Term
	rf.votedFor = args.CandidateId
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply, ret chan *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		ret <- reply
	} else {
		// if RPC call fails for any reason, vote is not received; do not update sender's term
		ret <- &RequestVoteReply{Term: args.Term, VoteGranted: false}
	}
}

func (rf *Raft) sendAppendEntry(peer int, args AppendEntryArgs, reply *AppendEntryReply, ret chan *AppendEntryReply) {
	ok := rf.peers[peer].Call("Raft.AppendEntry", args, reply)

	if ok {
		ret <- reply
	} else {
		// if RPC call fails for any reason, request is rejected; do not update sender's term
		ret <- &AppendEntryReply{Term: args.Term, Accepted: false}
	}
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
	index := len(rf.log)
	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, term, false
	}

	if command != nil {
		rf.log = append(rf.log, Log{term: term, value: command})
	} else {
		rf.restartTimer()
	}

	args := AppendEntryArgs{
		Term:         term,
		Leader:       rf.me,
		Value:        command,
		PrevLogTerm:  -1, // empty log
		PrevLogIndex: -1, // empty log
	}
	if L := len(rf.log); L > 0 {
		args.PrevLogTerm = rf.log[L-1].term
		args.PrevLogIndex = L - 1
	}

	ret := make(chan *AppendEntryReply)
	for peer := range rf.peers {
		go rf.sendAppendEntry(peer, args, &AppendEntryReply{}, ret)
	}

	count := 0
	for range rf.peers {
		reply := <-ret
		if reply.Term > term {
			term = reply.Term
			rf.follow(term, reply.Leader)
			break
		}

		if reply.Accepted {
			count += 1
		}
	}

	if command != nil && count > len(rf.peers)/2 {
		rf.applyCh <- ApplyMsg{Index: index, Command: command}
	}

	return index, term, isLeader
}

// Returns term and whether the candidate won the election
func (rf *Raft) StartElection() (int, bool) {
	term := rf.currentTerm + 1
	rf.currentTerm = term
	rf.votedFor = rf.me
	fmt.Printf("Candidate %v is starting election %v \n", rf.me, term)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: -1, // empty log
		LastLogTerm:  -1, // empty log
	}
	if L := len(rf.log); L > 0 {
		args.LastLogTerm = rf.log[L-1].term
		args.LastLogIndex = L - 1
	}

	ret := make(chan *RequestVoteReply, len(rf.peers))
	for peer := range rf.peers {
		go rf.sendRequestVote(peer, args, &RequestVoteReply{}, ret)
	}

	count := 0
	for i := range rf.peers {
		// select {
		// // received from AppendEntries RPC if newer leader is found
		// case term := <-rf.notifyFollow:
		// 	return term, false

		// // case <-rf.electiontimeout:
		// // 	return term, false

		// case reply := <-ret:
		reply := <-ret

		fmt.Printf("Election %v (%v/%v): server %v replied %v with term %v \n", term, i+1, len(rf.peers), reply.Sender, reply.VoteGranted, reply.Term)
		// If you are holding election for an old or same term, end elections
		if reply.Term > term {
			fmt.Println("Demoting self")
			rf.follow(reply.Term, reply.Leader)
			return reply.Term, false
		}

		if reply.VoteGranted {
			count += 1
		}
		// }

	}

	fmt.Printf("Candidate %v election received %v votes", rf.me, count)
	if count > len(rf.peers)/2 {
		rf.currentTerm += 1
		term = rf.currentTerm
		rf.leader = rf.me
		return term, true
	} else {
		rf.restartTimer()
	}
	return term, false
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.restartTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) follow(term, leader int) {
	rf.leader = leader
	rf.currentTerm = term
	rf.votedFor = -1
	rf.restartTimer()
}

// func (rf *Raft) timeoutHandler() {
// 	const (
// 		startElection time.Duration = 500 * time.Millisecond
// 		sendBeat                    = 50 * time.Millisecond
// 	)
// 	rf.timer.Stop()
// 	_, isleader := rf.GetState()
// 	if isleader {
// 		rf.timer = time.AfterFunc(sendBeat, func() { rf.Start(nil) })
// 	} else {
// 		rf.timer = time.AfterFunc(startElection, func() { rf.StartElection() })
// 	}
// }

func (rf *Raft) restartTimer() {
	go func() {
		if rf.timer != nil {
			rf.timer.Stop()
		}

		_, isleader := rf.GetState()
		if isleader {
			rf.timer = time.AfterFunc(
				time.Duration(5+rand.Intn(5))*time.Millisecond,
				func() { rf.Start(nil) }) // Heartbeat
		} else {
			rf.timer = time.AfterFunc(
				time.Duration(500+rand.Intn(1000))*time.Millisecond,
				func() { rf.StartElection() })
		}
	}()
}
