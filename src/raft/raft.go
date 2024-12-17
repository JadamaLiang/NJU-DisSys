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

// LogEntry represents a single log entry in the Raft log.
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	state            string // "Follower", "Candidate", "Leader"
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	electionTimer *time.Timer
	applyCh       chan ApplyMsg
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
	Term         int // Candidate's term
	CandidateID  int // Candidate requesting vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // CurrentTerm for the candidate to update itself
	VoteGranted bool // True means candidate received the vote
}

// AppendEntriesArgs defines the arguments for AppendEntries RPC.
type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderID int // Leader ID
}

// AppendEntriesReply defines the reply for AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  // Current term
	Success bool // True if follower accepted the heartbeat
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// Reject votes for outdated terms
	if args.Term < rf.currentTerm {
		return
	}

	// Update term and convert to follower if a higher term is seen
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "Follower"
	}

	// Grant vote if no vote has been cast or candidate's log is at least as up-to-date
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.resetElectionTimer()
	}

	reply.Term = rf.currentTerm
}

// AppendEntries handles the AppendEntries RPC (heartbeat).
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// Reject if leader's term is outdated
	if args.Term < rf.currentTerm {
		return
	}

	// Update term and convert to follower if necessary
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "Follower"
	}

	rf.resetElectionTimer()
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

// sendAppendEntries sends an AppendEntries RPC to a peer.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// startElection starts an election by transitioning to Candidate state.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = "Candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	votes := 1
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.lastLogTerm(),
				}
				reply := RequestVoteReply{}
				if rf.sendRequestVote(peer, args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = "Follower"
						rf.votedFor = -1
						return
					}
					if reply.VoteGranted && rf.state == "Candidate" {
						votes++
						if votes > len(rf.peers)/2 {
							rf.state = "Leader"
							rf.resetHeartbeatTimer()
						}
					}
				}
			}(peer)
		}
	}
}

// sendHeartbeats sends periodic heartbeats from the leader.
func (rf *Raft) sendHeartbeats() {
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderID: rf.me,
				}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(peer, &args, &reply)
			}(peer)
		}
	}
}

// resetElectionTimer resets the randomized election timeout.
func (rf *Raft) resetElectionTimer() {
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTimer = time.AfterFunc(rf.electionTimeout, func() {
		rf.startElection()
	})
}

// resetHeartbeatTimer sends heartbeats periodically for the leader.
func (rf *Raft) resetHeartbeatTimer() {
	for rf.state == "Leader" {
		time.Sleep(50 * time.Millisecond)
		rf.sendHeartbeats()
	}
}

// lastLogTerm returns the term of the last log entry.
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
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
	isLeader := true

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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		state:         "Follower",
		applyCh:       applyCh,
		votedFor:      -1,
		electionTimer: time.NewTimer(time.Duration(150+rand.Intn(150)) * time.Millisecond),
	}
	go func() {
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			if state == "Leader" {
				rf.sendHeartbeats()
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	rf.resetElectionTimer()
	return rf
}
