package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

//---type definitions---//

// represents the message used to apply a command to the state machine.
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// represents a log entry in the Raft log.
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// raft structure
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	state       int
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	voteCount int
	timer     *time.Timer

	applyCh  chan ApplyMsg
	leaderCh chan bool
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs represents the arguments for the AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply represents the reply from the AppendEntries RPC.
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

const (
	FOLLOWER              = 0
	CANDIDATE             = 1
	LEADER                = 2
	INTERVAL_OF_HEARTBEAT = 50
	INTERVAL_OF_ELECTION  = 150
)

//---raft methods---//

// GetState returns the current term and whether the server is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// persist saves the state of the Raft server to stable storage.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
}

// readPersist restores the server's persistent state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// RequestVote handles a RequestVote RPC.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.state, rf.votedFor = args.Term, FOLLOWER, -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastIndex, lastTerm := rf.getLastIndex(), rf.getLastTerm()
		if lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = FOLLOWER
			rf.timer.Reset(getRandomInterval())
		}
	}
}

// sendRequestVote sends a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// sendVoteRequestsToAllPeers sends RequestVote RPCs to all other servers.
func (rf *Raft) sendVoteRequestsToAllPeers() {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != CANDIDATE || args.Term != rf.currentTerm {
					return
				}
				if reply.VoteGranted {
					rf.voteCount++
					if rf.voteCount > len(rf.peers)/2 && rf.state == CANDIDATE {
						rf.leaderCh <- true
					}
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm, rf.state, rf.votedFor = reply.Term, FOLLOWER, -1
					rf.persist()
				}
			}(i)
		}
	}
}

func (rf *Raft) getLastIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

// AppendEntries handles an AppendEntries RPC.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.state, rf.votedFor = args.Term, FOLLOWER, -1
	}

	reply.Term = args.Term
	rf.timer.Reset(getRandomInterval())

	lastIndex := rf.getLastIndex()
	if args.PrevLogIndex > lastIndex {
		reply.NextIndex = lastIndex + 1
	} else {
		if args.PrevLogIndex >= 0 {
			term := rf.log[args.PrevLogIndex].Term
			if args.PrevLogTerm != term {
				for i := args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.log[i].Term != term {
						reply.NextIndex = i + 1
						reply.Term = rf.currentTerm
						return
					}
				}
			} else {
				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
				lastIndex = rf.getLastIndex()
				reply.NextIndex = lastIndex + 1
				reply.Success = true
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
			rf.applyCh <- msg
			rf.lastApplied = i
		}
	}
}

// sendAppendEntries sends an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// sendLogEntriesToFollowers sends AppendEntries RPCs to all followers.
func (rf *Raft) sendLogEntriesToFollowers() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.nextIndex[i] - 1,
			}
			if args.PrevLogIndex > -1 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			}
			args.Entries = rf.log[rf.nextIndex[i]:]

			go func(i int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				if rf.sendAppendEntries(i, args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if args.Term != rf.currentTerm || rf.state != LEADER {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.currentTerm, rf.state, rf.votedFor = reply.Term, FOLLOWER, -1
						rf.persist()
						return
					}

					if reply.Success {
						if len(args.Entries) > 0 {
							rf.matchIndex[i], rf.nextIndex[i] = args.PrevLogIndex+len(args.Entries), rf.matchIndex[i]+1
						}
						for i := rf.commitIndex + 1; i <= rf.getLastIndex(); i++ {
							count := 1
							for j := range rf.peers {
								if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
									count++
								}
							}
							if count > len(rf.peers)/2 {
								rf.commitIndex = i
							}
						}
					} else {
						rf.nextIndex[i] = reply.NextIndex
					}
				}
			}(i, args)
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.state = CANDIDATE
	rf.persist()
	rf.mu.Unlock()

	go rf.sendVoteRequestsToAllPeers()
}

func getRandomInterval() time.Duration {
	return time.Duration(rand.Intn(INTERVAL_OF_ELECTION)+INTERVAL_OF_ELECTION) * time.Millisecond
}

// Start begins the agreement on the next log entry.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.getLastIndex() + 1
		rf.log = append(rf.log, LogEntry{Term: term, Command: command, Index: index})
		rf.persist()
	}
	return index, term, isLeader
}

// Kill is called when the Raft server will not be used again.
func (rf *Raft) Kill() {
	// Optional cleanup code
}

// Make creates a new Raft server instance.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       FOLLOWER,
		currentTerm: 0,
		timer:       time.NewTimer(getRandomInterval()),
		applyCh:     applyCh,
		leaderCh:    make(chan bool, 10),
		log:         append([]LogEntry{}, LogEntry{Term: 0, Index: 0}),
		commitIndex: 0,
		lastApplied: 0,
	}

	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				select {
				case <-rf.timer.C:
					rf.startElection()
					rf.mu.Lock()
					rf.timer.Reset(getRandomInterval())
					rf.mu.Unlock()
				}
			case CANDIDATE:
				select {
				case <-rf.timer.C:
					rf.startElection()
					rf.mu.Lock()
					rf.timer.Reset(getRandomInterval())
					rf.mu.Unlock()
				case <-rf.leaderCh:
					rf.mu.Lock()
					if rf.voteCount > len(rf.peers)/2 {
						rf.state = LEADER
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						lastIndex := rf.getLastIndex()
						for i := range rf.peers {
							if i != rf.me {
								rf.nextIndex[i] = lastIndex + 1
								rf.matchIndex[i] = 0
							}
						}
					}
					rf.mu.Unlock()
				}
			case LEADER:
				select {
				case <-time.After(INTERVAL_OF_HEARTBEAT * time.Millisecond):
					rf.sendLogEntriesToFollowers()
				}
			}
		}
	}()
	return rf
}
