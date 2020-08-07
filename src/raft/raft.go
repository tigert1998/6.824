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
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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
	log         []LogEntry

	// volatile, for all servers
	commitIndex int32
	lastApplied int32

	// volatile, only for leaders
	nextIndex  []int
	matchIndex []int

	// others
	lastHeartBeat   atomic.Value
	electionTimeout time.Duration
	roleMtx         sync.RWMutex // guards role, currentTerm, votedFor, nextIndex, matchIndex
	role            int
	logMtx          sync.RWMutex // guards log
	applyCh         chan ApplyMsg
	applyNotifier   chan struct{}
	leaderNotifier  chan struct{}

	// utilities
	r *rand.Rand
}

func (rf *Raft) generateElectionTimeout() {
	rf.electionTimeout = time.Duration(800+rf.r.Int()%200) * time.Millisecond
}

func (rf *Raft) shouldAttendElection() bool {
	lastHeartBeat := rf.lastHeartBeat.Load().(time.Time)
	return time.Since(lastHeartBeat) >= rf.electionTimeout
}

func (rf *Raft) updateCommitIndex(value int32) {
	for {
		prevValue := atomic.LoadInt32(&rf.commitIndex)
		if prevValue >= value {
			break
		}
		if atomic.CompareAndSwapInt32(&rf.commitIndex, prevValue, value) {
			break
		}
	}
	rf.applyNotifier <- struct{}{}
}

func (rf *Raft) applyLoop() {
	for {
		<-rf.applyNotifier
		if rf.killed() {
			return
		}
		commitIndex := atomic.LoadInt32(&rf.commitIndex)
		for i := rf.lastApplied + 1; i <= commitIndex; i++ {
			rf.logMtx.RLock()
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: int(i),
			}
			rf.logMtx.RUnlock()
			rf.applyCh <- msg
		}
		rf.lastApplied = commitIndex
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = LEADER

	rf.logMtx.RLock()
	logLen := len(rf.log)
	rf.logMtx.RUnlock()

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = logLen
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = logLen - 1
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = FOLLOWER
}

func (rf *Raft) logMatch(index int, term int) bool {
	// needs to guard log before calling this routine
	if index < 0 || index >= len(rf.log) {
		return false
	} else {
		return rf.log[index].Term == term
	}
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) getRoleTermVote() (int, int, int) {
	rf.roleMtx.RLock()
	defer rf.roleMtx.RUnlock()
	return rf.role, rf.currentTerm, rf.votedFor
}

func (rf *Raft) getLogIndexTerm(index int, last bool) (int, int) {
	rf.logMtx.RLock()
	defer rf.logMtx.RUnlock()

	if last {
		index = len(rf.log) - 1
	}

	return index, rf.log[index].Term
}

func (rf *Raft) GetState() (int, bool) {
	role, term, _ := rf.getRoleTermVote()
	return term, role == LEADER
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) error {
	if data == nil || len(data) < 1 {
		// bootstrap without any state
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	e := d.Decode(&currentTerm)
	if e != nil {
		return e
	}
	e = d.Decode(&votedFor)
	if e != nil {
		return e
	}
	e = d.Decode(&log)
	if e != nil {
		return e
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	return nil
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
		return
	}

	// whose log is more up-to-date
	lastLogIndex, lastLogTerm := rf.getLogIndexTerm(0, true)
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.lastHeartBeat.Store(time.Now())

	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		if rf.votedFor == -1 && rf.role == FOLLOWER {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}

	if reply.VoteGranted {
		log.Printf("[term #%v] vote [%v] <- [%v]", args.Term, args.CandidateID, rf.me)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.roleMtx.Lock()
	defer rf.roleMtx.Unlock()

	rf.lastHeartBeat.Store(time.Now())

	if args.Term < rf.currentTerm {
		// obsolete package
		reply.Success = false
	} else {
		rf.logMtx.Lock()
		reply.Success = rf.logMatch(args.PrevLogIndex, args.PrevLogTerm)
		if reply.Success {
			var i int
			for i = 0; i < len(args.Entries) && args.PrevLogIndex+1+i < len(rf.log); i++ {
				if args.Entries[i].Term != rf.log[args.PrevLogIndex+1+i].Term {
					break
				}
			}
			rf.log = append(rf.log[:args.PrevLogIndex+1+i], args.Entries[i:]...)
			rf.logMtx.Unlock()
			rf.updateCommitIndex(int32(minInt(len(rf.log)-1, args.LeaderCommit)))
		} else {
			rf.log = rf.log[:minInt(args.PrevLogIndex, len(rf.log))]
			rf.logMtx.Unlock()
		}

		if args.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
		}
	}

	reply.Term = rf.currentTerm
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
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.roleMtx.Lock()
	defer rf.roleMtx.Unlock()

	if rf.role != LEADER {
		return 0, rf.currentTerm, false
	}

	rf.logMtx.Lock()
	defer rf.logMtx.Unlock()

	index := len(rf.log)
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.matchIndex[rf.me] = index

	log.Printf("[term #%v] issue command [%v], index = %v", rf.currentTerm, rf.me, index)

	return index, rf.currentTerm, true
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
	rf.applyNotifier <- struct{}{}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) campaign() {
	// attend election
	rf.roleMtx.Lock()
	defer rf.roleMtx.Unlock()

	if rf.role == LEADER {
		return
	}
	if !rf.shouldAttendElection() {
		return
	}

	rf.votedFor = rf.me
	rf.currentTerm++
	rf.role = CANDIDATE
	campaignTerm := rf.currentTerm

	log.Printf("[term #%v] [%v] starts election", campaignTerm, rf.me)

	lastLogIndex, lastLogTerm := rf.getLogIndexTerm(0, true)

	args := RequestVoteArgs{
		Term:         campaignTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	var numVotes int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}
		if i == rf.me {
			continue
		}

		go func(target int) {
			var role, term int

			reply := RequestVoteReply{}

			const Retries = 5
			var i int
			for i = 0; i < Retries && !rf.peers[target].Call("Raft.RequestVote", &args, &reply); i++ {
				if rf.killed() {
					return
				}
				role, term, _ = rf.getRoleTermVote()
				if campaignTerm < term || role != CANDIDATE {
					return
				}
			}
			if i >= Retries {
				return
			}

			if reply.VoteGranted {
				if atomic.AddInt32(&numVotes, 1) == int32(rf.majority()) {
					rf.roleMtx.Lock()
					if rf.role == FOLLOWER && campaignTerm < rf.currentTerm {
						// do nothing, simply give up
						rf.roleMtx.Unlock()
					} else if rf.currentTerm == campaignTerm {
						rf.becomeLeader()
						rf.roleMtx.Unlock()
						rf.sendHeartBeat()
						rf.leaderNotifier <- struct{}{}
					} else {
						log.Fatalf("[term #%v] [%v] invalid case when winning the campaign: campaignTerm = %v", rf.currentTerm, rf.me, campaignTerm)
						rf.roleMtx.Unlock()
					}
				}
			} else if reply.Term > campaignTerm {
				rf.roleMtx.Lock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
				rf.roleMtx.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) sendHeartBeat() {
	// send heart beat
	role, _, _ := rf.getRoleTermVote()
	if role != LEADER {
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.killed() {
			return
		}

		go func(target int) {
			rf.roleMtx.RLock()
			if rf.role != LEADER {
				rf.roleMtx.RUnlock()
				return
			}
			rf.logMtx.RLock()
			const MaxEntries = 10
			prevLogIndex, prevLogTerm := rf.getLogIndexTerm(rf.nextIndex[target]-1, false)
			sendLogFrom := rf.nextIndex[target]
			sendLogTo := minInt(rf.nextIndex[target]+MaxEntries, len(rf.log))

			entries := make([]LogEntry, sendLogTo-sendLogFrom)
			copy(entries, rf.log[sendLogFrom:sendLogTo])
			rf.logMtx.RUnlock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: int(atomic.LoadInt32(&rf.commitIndex)),
			}
			rf.roleMtx.RUnlock()

			reply := AppendEntriesReply{}
			if !rf.peers[target].Call("Raft.AppendEntries", &args, &reply) {
				return
			}

			rf.roleMtx.Lock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			} else if rf.role == LEADER && rf.currentTerm == args.Term {
				if reply.Success {
					rf.matchIndex[target] = sendLogTo - 1
					rf.nextIndex[target] = sendLogTo

					tmp := make([]int, len(rf.peers))
					copy(tmp, rf.matchIndex)
					sort.Ints(tmp)
					newCommitIndex := tmp[len(tmp)-rf.majority()]
					rf.logMtx.RLock()
					if rf.log[newCommitIndex].Term == rf.currentTerm {
						rf.updateCommitIndex(int32(newCommitIndex))
					}
					rf.logMtx.RUnlock()
				} else {
					rf.nextIndex[target] = maxInt(rf.nextIndex[target]-1, 1)
				}
			}
			rf.roleMtx.Unlock()

			var actionStr string
			var successStr string
			if sendLogFrom == sendLogTo {
				actionStr = "heart beat"
			} else {
				actionStr = "send log"
			}
			if reply.Success {
				successStr = "success"
			} else {
				successStr = "fail"
			}
			log.Printf("[term #%v] %v [%v] -> [%v], range [%v, %v), %v", args.Term, actionStr, rf.me, target, sendLogFrom, sendLogTo, successStr)
		}(i)
	}
}

func (rf *Raft) eventLoop() {
	rf.generateElectionTimeout()

	for {
		if rf.killed() {
			return
		}
		_, isLeader := rf.GetState()
		if isLeader {
			time.Sleep(100 * time.Millisecond)
			rf.sendHeartBeat()
		} else {
			select {
			case <-time.After(rf.electionTimeout):
				if rf.shouldAttendElection() {
					if rf.killed() {
						return
					}
					rf.generateElectionTimeout()
					rf.campaign()
				}
			case <-rf.leaderNotifier:
				// do nothing, just wake up
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
var timeSeed int
var once sync.Once

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags((log.LstdFlags | log.Lmicroseconds) &^ log.Ldate)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{LogEntry{Term: 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	e := rf.readPersist(persister.ReadRaftState())
	if e != nil {
		log.Fatalf("%v", e)
	}

	rf.lastHeartBeat.Store(time.Time{})
	rf.role = FOLLOWER
	rf.applyCh = applyCh
	rf.applyNotifier = make(chan struct{}, 10)
	rf.leaderNotifier = make(chan struct{})

	once.Do(func() {
		timeSeed = int(time.Now().Unix())
		log.Printf("time seed = %v", timeSeed)
	})

	rf.r = rand.New(rand.NewSource(int64(rf.me * timeSeed)))

	go rf.eventLoop()
	go rf.applyLoop()

	return rf
}
