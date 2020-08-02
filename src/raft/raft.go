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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// non-volatile, for all servers
	currentTerm int
	votedFor    int
	logMtx      sync.RWMutex
	log         []LogEntry

	// volatile, for all servers
	commitIndex int
	lastApplied int

	// volatile, only for leaders
	nextIndex  []int
	matchIndex []int

	// others
	lastHeartBeat atomic.Value
	roleMtx       sync.RWMutex // guards role, currentTerm, votedFor
	role          int

	// utilities
	r *rand.Rand
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) getRoleTermVote() (int, int, int) {
	rf.roleMtx.RLock()
	defer rf.roleMtx.RUnlock()
	return rf.role, rf.currentTerm, rf.votedFor
}

func (rf *Raft) getLogIndexTerm(index int) (int, int) {
	rf.logMtx.RLock()
	defer rf.logMtx.RUnlock()

	if index == -1 {
		logLen := len(rf.log)
		if logLen == 0 {
			return -1, 0
		} else {
			return logLen - 1, rf.log[logLen-1].Term
		}
	} else {
		return index, rf.log[index].Term
	}
}

func (rf *Raft) GetState() (int, bool) {
	role, term, _ := rf.getRoleTermVote()
	return term, role == LEADER
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

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.roleMtx.Lock()
	defer rf.roleMtx.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		rf.lastHeartBeat.Store(time.Now())
		reply.Term = rf.currentTerm
		if rf.votedFor == -1 && rf.role == FOLLOWER {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		rf.lastHeartBeat.Store(time.Now())
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lastHeartBeat.Store(time.Now())

	rf.roleMtx.Lock()
	defer rf.roleMtx.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
		reply.Success = true
	} else if args.Term == rf.currentTerm {
		reply.Success = true
	} else if args.Term < rf.currentTerm {
		reply.Success = false
	}

	reply.Term = rf.currentTerm
}

//
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
//

//
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

func (rf *Raft) campaign() {
	// attend election
	var campaignTerm int
	{
		rf.roleMtx.Lock()

		if rf.role == CANDIDATE {
			log.Printf("[%v] previous election tied. election restarts", rf.me)
		} else if rf.role == LEADER {
			log.Fatalf("[%v] invalid role in the beginning of the campaign: role == LEADER", rf.me)
		}
		rf.votedFor = rf.me
		rf.currentTerm++
		rf.role = CANDIDATE
		campaignTerm = rf.currentTerm

		rf.roleMtx.Unlock()
	}
	log.Printf("[%v] starts election for term #%v", rf.me, campaignTerm)

	lastLogIndex, lastLogTerm := rf.getLogIndexTerm(-1)

	args := RequestVoteArgs{
		Term:         campaignTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}

	ch := make(chan int, len(rf.peers))
	totThreads := 0

	numVotes := 1
	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}
		role, term, _ := rf.getRoleTermVote()
		if role != CANDIDATE || campaignTerm < term {
			return
		}
		if i == rf.me {
			continue
		}

		totThreads++
		go func(target int) {
			var role, term int

			for !rf.peers[target].Call("Raft.RequestVote", &args, &reply) {
				role, term, _ = rf.getRoleTermVote()
				if rf.killed() {
					return
				}
				if role != CANDIDATE || campaignTerm < term {
					return
				}
				time.Sleep(25 * time.Millisecond)
			}

			if reply.VoteGranted {
				numVotes++
				if numVotes >= rf.majority() {
					rf.roleMtx.Lock()
					if rf.role == FOLLOWER && campaignTerm < rf.currentTerm {
						// do nothing, simply give up
					} else if rf.currentTerm == campaignTerm {
						rf.role = LEADER
					} else {
						log.Fatalf("[%v] invalid case when winning the campaign: currentTerm = %v, campaignTerm = %v", rf.me, rf.currentTerm, campaignTerm)
					}
					rf.roleMtx.Unlock()
				}
			} else if reply.Term > campaignTerm {
				rf.roleMtx.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.role = FOLLOWER
				}
				rf.roleMtx.Unlock()
			}
			ch <- 0
		}(i)
	}

	for i := 0; i < totThreads; i++ {
		<-ch
	}
}

func (rf *Raft) sendHeartBeat() {
	// send heart beat

	rf.roleMtx.RLock()
	if rf.role != LEADER {
		rf.roleMtx.RUnlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      []LogEntry{},
		LeaderCommit: -1,
	}
	rf.roleMtx.RUnlock()

	reply := AppendEntriesReply{}

	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.killed() {
			return
		}

		go func(target int) {
			rf.peers[target].Call("Raft.AppendEntries", &args, &reply)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) checkHeartBeat() {
	generateElectionTimeout := func() time.Duration {
		return time.Duration(150+rf.r.Float32()*150) * time.Millisecond
	}

	electionTimeout := generateElectionTimeout()

	for {
		if rf.killed() {
			return
		}
		_, isLeader := rf.GetState()
		if isLeader {
			time.Sleep(15 * time.Millisecond)

			rf.sendHeartBeat()
		} else {
			time.Sleep(electionTimeout)

			lastHeartBeat := rf.lastHeartBeat.Load().(time.Time)
			if time.Since(lastHeartBeat) >= electionTimeout {
				for {
					if rf.killed() {
						return
					}
					electionTimeout = generateElectionTimeout()

					rf.campaign()

					role, _, _ := rf.getRoleTermVote()
					if role != CANDIDATE {
						break
					}
				}
			}
		}
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	// todo
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.readPersist(persister.ReadRaftState())

	rf.lastHeartBeat.Store(time.Time{})
	rf.role = FOLLOWER

	rf.r = rand.New(rand.NewSource(int64(rf.me * int(time.Now().Unix()))))

	go rf.checkHeartBeat()

	return rf
}
