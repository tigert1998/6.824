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

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

const (
	HEARTBEAT_WITH_LOG = 20
	HEARTBEAT          = 100
	ELECTION_BASE      = 800
	ELECTION_RAND      = 200
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// non-volatile, for all servers
	currentTerm       int
	votedFor          int
	lastIncludedIndex int
	lastIncludedTerm  int
	log               []LogEntry

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

	// for leaders
	lastHeartBeats []atomic.Value

	// utilities
	r *rand.Rand
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.roleMtx.Lock()
	defer rf.roleMtx.Unlock()
	rf.lastHeartBeat.Store(time.Now())
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
		Snapshot:      args.Data,
	}
	rf.applyCh <- msg
}

func (rf *Raft) generateElectionTimeout() {
	rf.electionTimeout = time.Duration(ELECTION_BASE+rf.r.Int()%ELECTION_RAND) * time.Millisecond
}

func (rf *Raft) shouldAttendElection() bool {
	lastHeartBeat := rf.lastHeartBeat.Load().(time.Time)
	return time.Since(lastHeartBeat) >= rf.electionTimeout
}

func (rf *Raft) updateCommitIndex(value int32) {
	atomicMaxInt32(&rf.commitIndex, value)
	rf.applyNotifier <- struct{}{}
}

func (rf *Raft) getLog(i int) *LogEntry {
	return &rf.log[i-rf.lastIncludedIndex-1]
}

func (rf *Raft) getLogs(from int, to int) []LogEntry {
	// [from, to)
	l := from - rf.lastIncludedIndex - 1
	r := to - rf.lastIncludedIndex - 1
	if from < 0 {
		return rf.log[:r]
	}
	if to < 0 {
		return rf.log[l:]
	}
	return rf.log[l:r]
}

func (rf *Raft) logLen() int {
	return len(rf.log) + rf.lastIncludedIndex + 1
}

func (rf *Raft) applyLoop() {
	for {
		<-rf.applyNotifier
		if rf.killed() {
			return
		}
		commitIndex := atomic.LoadInt32(&rf.commitIndex)
		for {
			rf.logMtx.RLock()
			i := atomic.LoadInt32(&rf.lastApplied) + 1
			if i <= commitIndex && atomic.CompareAndSwapInt32(&rf.lastApplied, i-1, i) {
				msg := ApplyMsg{
					CommandValid:  true,
					Command:       rf.getLog(int(i)).Command,
					CommandIndex:  int(i),
					SnapshotValid: false,
				}
				rf.logMtx.RUnlock()
				rf.applyCh <- msg
			} else {
				rf.logMtx.RUnlock()
				break
			}
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = LEADER

	rf.logMtx.RLock()
	logLen := rf.logLen()
	rf.logMtx.RUnlock()

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = logLen
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = logLen - 1

	for i := 0; i < len(rf.lastHeartBeats); i++ {
		rf.lastHeartBeats[i].Store(time.Time{})
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = FOLLOWER
}

func (rf *Raft) logMatch(index int, term int) bool {
	// needs to guard log before calling this routine
	if index < 0 || index >= rf.logLen() {
		return false
	} else {
		_, logTerm := rf.getLogIndexTerm(index, false)
		return logTerm == term
	}
}

func (rf *Raft) nextPossibleLogMatch(index int, term int) int {
	l := rf.lastIncludedIndex
	r := minInt(rf.logLen()-1, index)
	for l < r {
		mid := (l + r + 1) / 2
		_, midTerm := rf.getLogIndexTerm(mid, false)
		if midTerm <= term {
			l = mid
		} else {
			r = mid - 1
		}
	}
	ans, ansTerm := rf.getLogIndexTerm(l, false)
	if ans == index && ansTerm < term {
		ans--
	}
	return ans
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
	if last {
		index = rf.logLen() - 1
	}

	if index == rf.lastIncludedIndex {
		return index, rf.lastIncludedTerm
	}

	return index, rf.getLog(index).Term
}

func (rf *Raft) GetState() (int, bool) {
	role, term, _ := rf.getRoleTermVote()
	return term, role == LEADER
}

func (rf *Raft) getSerializedRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getSerializedRaftState())
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
	var lastIncludedIndex int
	var lastIncludedTerm int
	var log []LogEntry

	e := d.Decode(&currentTerm)
	if e != nil {
		return e
	}
	e = d.Decode(&votedFor)
	if e != nil {
		return e
	}
	e = d.Decode(&lastIncludedIndex)
	if e != nil {
		return e
	}
	e = d.Decode(&lastIncludedTerm)
	if e != nil {
		return e
	}
	e = d.Decode(&log)
	if e != nil {
		return e
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.log = log

	rf.lastApplied = int32(rf.lastIncludedIndex)
	rf.commitIndex = int32(rf.lastIncludedIndex)
	return nil
}

func (rf *Raft) trimLog(lastIncludedTerm int, lastIncludedIndex int) {
	rf.lastIncludedTerm = lastIncludedTerm
	if lastIncludedIndex+1 >= rf.logLen() {
		rf.log = []LogEntry{}
	} else if rf.logMatch(lastIncludedIndex, lastIncludedTerm) {
		rf.log = rf.log[lastIncludedIndex-rf.lastIncludedIndex:]
	} else {
		rf.log = []LogEntry{}
	}
	rf.lastIncludedIndex = lastIncludedIndex
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.roleMtx.Lock()
	defer rf.roleMtx.Unlock()
	rf.logMtx.Lock()
	defer rf.logMtx.Unlock()
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return false
	}
	atomicMaxInt32(&rf.lastApplied, int32(lastIncludedIndex))
	rf.updateCommitIndex(int32(lastIncludedIndex))
	rf.trimLog(lastIncludedTerm, lastIncludedIndex)
	rf.persister.SaveStateAndSnapshot(rf.getSerializedRaftState(), snapshot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.roleMtx.Lock()
	defer rf.roleMtx.Unlock()
	rf.logMtx.Lock()
	defer rf.logMtx.Unlock()
	log.Printf("[term #%d] snapshot [%d], last included %v", rf.currentTerm, rf.me, index)
	rf.trimLog(rf.getLog(index).Term, index)
	rf.persister.SaveStateAndSnapshot(rf.getSerializedRaftState(), snapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
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
	Term           int
	Success        bool
	RejectLogIndex int
	RejectLogTerm  int
}

type InstallSnapshotArgs struct {
	Term int
	// LeaderID
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
	rf.logMtx.RLock()
	lastLogIndex, lastLogTerm := rf.getLogIndexTerm(0, true)
	rf.logMtx.RUnlock()
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

			rf.persist()
		} else {
			reply.VoteGranted = false
		}
	} else {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.persist()
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
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		if args.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
		}
		reply.Term = rf.currentTerm

		rf.logMtx.Lock()
		reply.Success = rf.logMatch(args.PrevLogIndex, args.PrevLogTerm)
		if reply.Success {
			var i int
			for i = 0; i < len(args.Entries) && args.PrevLogIndex+1+i < rf.logLen(); i++ {
				if args.Entries[i].Term != rf.getLog(args.PrevLogIndex+1+i).Term {
					break
				}
			}
			if i < len(args.Entries) {
				rf.log = append(rf.getLogs(-1, args.PrevLogIndex+1+i), args.Entries[i:]...)
			}
			rf.updateCommitIndex(int32(minInt(rf.logLen()-1, args.LeaderCommit)))
		} else {
			rf.log = rf.getLogs(-1, minInt(args.PrevLogIndex, rf.logLen()))
			reply.RejectLogIndex = rf.nextPossibleLogMatch(args.PrevLogIndex, args.PrevLogTerm)
			_, rejectLogTerm := rf.getLogIndexTerm(reply.RejectLogIndex, false)
			reply.RejectLogTerm = rejectLogTerm
		}
		rf.persist()
		rf.logMtx.Unlock()
	}
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

	index := rf.logLen()
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.matchIndex[rf.me] = index

	rf.persist()

	log.Printf("[term #%v] issue command [%v], index = %v", rf.currentTerm, rf.me, index)
	currentTerm := rf.currentTerm

	// rf.sendHeartBeat()

	return index, currentTerm, true
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

	rf.logMtx.RLock()
	lastLogIndex, lastLogTerm := rf.getLogIndexTerm(0, true)
	rf.logMtx.RUnlock()

	args := RequestVoteArgs{
		Term:         campaignTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.persist()

	log.Printf("[term #%v] [%v] starts election", campaignTerm, rf.me)

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
					if rf.role == FOLLOWER || campaignTerm < rf.currentTerm {
						// do nothing, simply give up
						rf.roleMtx.Unlock()
					} else {
						rf.becomeLeader()
						rf.persist()
						rf.roleMtx.Unlock()

						rf.sendHeartBeat()
						rf.leaderNotifier <- struct{}{}
					}
				}
			} else if reply.Term > campaignTerm {
				rf.roleMtx.Lock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					rf.persist()
				}
				rf.roleMtx.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) calcNewCommitIndex() {
	tmp := make([]int, len(rf.peers))
	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	newCommitIndex := tmp[len(tmp)-rf.majority()]
	rf.logMtx.RLock()
	_, term := rf.getLogIndexTerm(newCommitIndex, false)
	if term == rf.currentTerm {
		rf.updateCommitIndex(int32(newCommitIndex))
	}
	rf.logMtx.RUnlock()
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

			if rf.nextIndex[target] < rf.lastIncludedIndex+1 {
				// InstallSnapshot
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				reply := InstallSnapshotReply{}
				rf.roleMtx.RUnlock()

				ret := rf.peers[target].Call("Raft.InstallSnapshot", &args, &reply)
				if ret {
					success := true
					rf.roleMtx.Lock()
					if reply.Term < rf.currentTerm {
						// do nothing
					} else if reply.Term > rf.currentTerm {
						success = false
						rf.becomeFollower(reply.Term)
						rf.persist()
					} else if rf.role == LEADER && rf.currentTerm == args.Term {
						rf.matchIndex[target] = maxInt(rf.matchIndex[target], args.LastIncludedIndex)
						rf.nextIndex[target] = rf.matchIndex[target] + 1
						rf.calcNewCommitIndex()
					}
					rf.roleMtx.Unlock()

					var resultStr string
					if ret {
						if success {
							resultStr = "success"
						} else {
							resultStr = "fail"
						}
					} else {
						resultStr = "lost"
					}
					log.Printf("[term #%v] install snapshot [%v] -> [%v], last included %v, %v", args.Term, rf.me, target, args.LastIncludedIndex, resultStr)
				}
			} else {
				// AppendEntries
				rf.logMtx.RLock()
				const MaxEntries = 100
				prevLogIndex, prevLogTerm := rf.getLogIndexTerm(rf.nextIndex[target]-1, false)
				sendLogFrom := rf.nextIndex[target]
				sendLogTo := minInt(rf.nextIndex[target]+MaxEntries, rf.logLen())

				if sendLogFrom == sendLogTo {
					if time.Since(rf.lastHeartBeats[target].Load().(time.Time)) < HEARTBEAT*time.Millisecond {
						rf.roleMtx.RUnlock()
						rf.logMtx.RUnlock()
						return
					}
				}
				rf.lastHeartBeats[target].Store(time.Now())

				entries := make([]LogEntry, sendLogTo-sendLogFrom)
				copy(entries, rf.getLogs(sendLogFrom, sendLogTo))
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
				ret := rf.peers[target].Call("Raft.AppendEntries", &args, &reply)
				if ret {
					rf.roleMtx.Lock()
					if reply.Term < rf.currentTerm {
						// do nothing
					} else if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
						rf.persist()
					} else if rf.role == LEADER && rf.currentTerm == args.Term {
						if reply.Success {
							rf.matchIndex[target] = maxInt(rf.matchIndex[target], sendLogTo-1)
							rf.nextIndex[target] = rf.matchIndex[target] + 1
							rf.calcNewCommitIndex()
						} else {
							rf.logMtx.RLock()
							nextIndex := rf.nextPossibleLogMatch(reply.RejectLogIndex, reply.RejectLogTerm)
							rf.logMtx.RUnlock()
							if nextIndex < rf.nextIndex[target] {
								rf.nextIndex[target] = maxInt(nextIndex, rf.matchIndex[target]+1)
							}
						}
						rf.persist()
					}
					rf.roleMtx.Unlock()
				}

				var actionStr string
				var resultStr string
				if sendLogFrom == sendLogTo {
					actionStr = "heart beat"
				} else {
					actionStr = "send log"
				}
				if ret {
					if reply.Success {
						resultStr = "success"
					} else {
						resultStr = "fail"
					}
				} else {
					resultStr = "lost"
				}
				log.Printf("[term #%v] %v [%v] -> [%v], range [%v, %v), %v", args.Term, actionStr, rf.me, target, sendLogFrom, sendLogTo, resultStr)
			}
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
			time.Sleep(HEARTBEAT_WITH_LOG * time.Millisecond)
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
	once.Do(func() {
		log.SetFlags((log.LstdFlags | log.Lmicroseconds) &^ log.Ldate)
		timeSeed = int(time.Now().Unix())
		log.Printf("time seed = %v", timeSeed)
	})

	log.Printf("[%v] reboots!", me)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.log = []LogEntry{}

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

	rf.lastHeartBeats = make([]atomic.Value, len(rf.peers))

	rf.r = rand.New(rand.NewSource(int64(rf.me * timeSeed)))

	go rf.eventLoop()
	go rf.applyLoop()

	return rf
}
