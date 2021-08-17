package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type logEntry struct {
	Term  int
	Index int
}

const (
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent
	currentTerm int
	voteFor     int
	log         []logEntry

	//volatile
	myState     int
	commitIndex int
	lastApplied int

	//volatile on leaders
	nextIndex  []int
	matchIndex []int

	//duration time
	heartbeatTimer *time.Timer
	electionTime   *time.Timer

	//communicate
	applyCh chan ApplyMsg
}

func StableHeartbeatTimeout() time.Duration {

	return 5 * time.Millisecond
}
func RandomizedElectionTimeout() time.Duration {
	var d = rand.Intn(100) + 40
	return time.Duration(d) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.myState == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidateid  int
	Lastlogindex int
	Lastlogterm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int //leader's Term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching  prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntriesRpc(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,Term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.myState, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}
	log, ok := rf.getLog(args.PrevLogIndex)

	if !ok || log.Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	rf.electionTime.Reset(RandomizedElectionTimeout())
	rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRpc", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,Term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v",
		rf.me, rf.myState, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	if rf.currentTerm > args.Term || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.Candidateid) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ChangeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	//candidate's log is not update to receiver's
	if !rf.isLogUpToDate(args.Lastlogterm, args.Lastlogindex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.voteFor = args.Candidateid
	rf.electionTime.Reset(RandomizedElectionTimeout())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 0,
		voteFor:     -1,
		log:         make([]logEntry, 1),
		myState:     FOLLOWER,
		commitIndex: 0,
		lastApplied: 0,

		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTime:   time.NewTimer(RandomizedElectionTimeout()),
		applyCh:        applyCh,
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLog.Index + 1
		if i != rf.me {

		}
	}
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()
	return rf
}

func (rf *Raft) startElection() {
	request := rf.genRequestVoteRequst()
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)
	grantedVotes := 1
	rf.voteFor = rf.me
	rf.persist()
	rf.electionTime.Reset(RandomizedElectionTimeout())
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			DPrintf("{Node %v} starts to send  RequestVoteRequest %v", rf.me, request)
			if rf.sendRequestVote(peer, &request, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in Term %v", rf.me, reply, peer, request, rf.currentTerm)
				if reply.Term == rf.currentTerm && rf.myState == CANDIDATE {
					if reply.VoteGranted {
						grantedVotes++
						DPrintf("{Node %v} gets votes %v", rf.me, grantedVotes)
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in Term %v", rf.me, rf.currentTerm)
							rf.ChangeState(LEADER)
							rf.BroadcastHeartbeat(true)
						}
					}
				} else if reply.Term > rf.currentTerm {
					DPrintf("{Node %v} finds a new leader {Node %v} with Term %v and steps down in Term %v", rf.me, peer, reply.Term, rf.currentTerm)
					rf.ChangeState(FOLLOWER)
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.persist()

				}

			}
		}(peer)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTime.C:
			DPrintf("election start")
			rf.mu.Lock()
			rf.ChangeState(CANDIDATE)
			rf.currentTerm += 1
			rf.startElection()
			rf.electionTime.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			DPrintf("Heartbeat")
			rf.mu.Lock()
			if rf.myState == LEADER {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) getFirstLog() logEntry {
	return rf.log[0]
}

func (rf *Raft) getLastLog() logEntry {
	lastIndex := len(rf.log) - 1
	return rf.log[lastIndex]
}

func (rf *Raft) ChangeState(state int) {
	rf.myState = state
}

func (rf *Raft) isLogUpToDate(candidateTerm int, candidateIndex int) bool {
	lastLog := rf.getLastLog()
	lastIndex := lastLog.Index
	if lastLog.Term > candidateTerm {
		return false
	}
	if lastLog.Term == candidateTerm {
		if candidateIndex < lastIndex {
			return false
		}
	}
	return true

}

func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		if isHeartbeat {
			rf.electionTime.Reset(RandomizedElectionTimeout())
			args := &AppendEntriesArgs{
				Term:         0,
				LeaderId:     0,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: 0,
			}
			reply := &AppendEntriesReply{
				Term:    0,
				Success: false,
			}
			go rf.sendAppendEntries(i, args, reply)
		}
	}

}

func (rf *Raft) genRequestVoteRequst() RequestVoteArgs {
	request := RequestVoteArgs{
		Term:         rf.currentTerm,
		Candidateid:  rf.me,
		Lastlogindex: rf.getLastLog().Index,
		Lastlogterm:  rf.getLastLog().Term,
	}
	return request
}

func (rf *Raft) applier() {

}

func (rf *Raft) getLog(index int) (log logEntry, ok bool) {
	ok = true
	if index >= len(rf.log) {
		ok = false
	}
	log = rf.log[index]
	return
}
