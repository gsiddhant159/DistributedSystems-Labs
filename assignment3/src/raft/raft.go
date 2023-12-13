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
	"labrpc"
	"sync"
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
	votedFor    int // index into peers[] //-1 is null value
	log         []Log

	// Volatile
	commitIndex int
	lastApplied int

	// as a Leader (must be reinitialised each term)
	nextIndex  []int
	matchIndex []int
}
type Log struct {
	term  int
	value interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	return term, isleader
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
	// Your data here.
	term         int // candidate’s term
	candidateId  int // candidate requesting vote
	lastLogIndex int // index of candidate’s last log entry (§5.4)
	lastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	term        int
	voteGranted bool
}

type AppendEntryArgs struct {
	term         int
	prevLogTerm  int
	prevLogIndex int
	value        interface{}
}

type AppendEntryReply struct {
	term int
	ok   bool
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	reply.term = args.term
	if rf.currentTerm > args.term { // Reject old leader?
		reply.ok = false
		reply.term = rf.currentTerm
		return
	}

	// Heartbeat check
	if args.value == nil {
		// rf.ECGchan <- true // means heartbeat detected
		reply.ok = true
		return
	}

	// Checking if previous entry in my log matches previous entry of incoming log
	L := len(rf.log)
	if L > 0 &&
		(rf.log[L-1].term != args.prevLogTerm ||
			L-1 != args.prevLogIndex) {
		reply.ok = false
		return
	}

	// Else, accept incoming request
	rf.log = append(rf.log, Log{term: args.term, value: args.value})
	reply.ok = true

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	reply.term = args.term
	if rf.currentTerm > args.term { // Reject old leader?
		reply.voteGranted = false
		reply.term = rf.currentTerm
		return
	}

	if vf := rf.votedFor; vf != -1 && vf != args.candidateId {
		reply.voteGranted = false
		return
	}

	if rf.log[len(rf.log)-1].term > args.lastLogTerm ||
		len(rf.log)-1 > args.lastLogIndex {
		reply.voteGranted = false
		return
	}

	reply.voteGranted = true
	reply.term = args.term
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

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := true

	rf.log = append(rf.log, Log{term: term, value: command})

	args := AppendEntryArgs{
		term:         rf.currentTerm,
		prevLogTerm:  -1,
		prevLogIndex: -1,
		value:        command,
	}

	L := len(rf.log)
	if L > 0 {
		args.prevLogTerm = rf.log[L-1].term
		args.prevLogIndex = L - 1
	}

	reply := AppendEntryReply{}

	count := 0
	for peer := range rf.peers {
		go func(peer int) {
			rf.peers[peer].Call("Raft.AppendEntry", args, &reply)

			if reply.ok {
				count += 1
			}
			if reply.term > term {
				term = reply.term
				isLeader = false
			}
		}(peer)
	}

	// if count > len(rf.peers)/2 {
	// 	rf.applyCh(ApplyMsg{Index:index, Command: command})
	// }

	return index, term, isLeader
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
	//go timeout(rf.ECGchan)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Possibly initalise a server entering the pool with the values of current leader

	// go rf.ECG()

	return rf
}

// func (rf *Raft) ECG() {

// }
