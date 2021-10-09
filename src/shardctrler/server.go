package shardctrler

import (
	"bytes"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"

	"github.com/google/uuid"
)

const (
	JOIN  = iota
	LEAVE = iota
	MOVE  = iota
	QUERY = iota
)

const APPLYLOOP_WAKEUP = 500

type lockTableItem struct {
	ch chan channelContent
	id string
}

type ShardCtrler struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg
	dead      int32

	maxraftstate int

	lockTable   map[int]lockTableItem
	clientTable map[string]int64

	configs []Config // indexed by config num
}

type Op struct {
	Op       int
	ID       string
	ClientID string
	TS       int64
	Servers  map[int][]string // Join
	GIDs     []int            // Leave
	Shard    int              // Move
	GID      int              // Move
	Num      int              // Query
}

type GIDCount struct {
	gid   int
	count int
}

type byCount []GIDCount

func (a byCount) Len() int      { return len(a) }
func (a byCount) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byCount) Less(i, j int) bool {
	return a[i].count > a[j].count || (a[i].count == a[j].count && a[i].gid < a[j].gid)
}

type byGID []int

func (a byGID) Len() int      { return len(a) }
func (a byGID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byGID) Less(i, j int) bool {
	return a[i] < a[j]
}

type channelContent struct {
	wrongLeader bool
	config      *Config
}

func (sc *ShardCtrler) rebalance(shards [NShards]int, groups map[int][]string) [NShards]int {
	if len(groups) == 0 {
		return [NShards]int{}
	}

	countsMap := map[int]int{}
	for _, gid := range shards {
		if gid == 0 {
			continue
		}
		count, ok := countsMap[gid]
		if ok {
			countsMap[gid] = count + 1
		} else {
			countsMap[gid] = 1
		}
	}

	avg := NShards / len(groups)
	mod := NShards % len(groups)

	var ret [NShards]int
	if len(countsMap) == 0 {
		// config num = 1
		gids := make([]int, 0, len(groups))
		for k := range groups {
			gids = append(gids, k)
		}
		sort.Sort(byGID(gids))

		idx := 0
		for i, gid := range gids {
			tot := avg
			if i < mod {
				tot += 1
			}
			for i := 0; i < tot; i++ {
				ret[idx] = gid
				idx += 1
			}
		}
	} else {
		countsArr := []GIDCount{}
		for gid, count := range countsMap {
			_, ok := groups[gid]
			if ok {
				countsArr = append(countsArr, GIDCount{gid, count})
			}
		}
		sort.Sort(byCount(countsArr))

		incArr := []GIDCount{}
		dec := map[int]int{}

		for i, pair := range countsArr {
			v := avg - pair.count
			if i < mod {
				v += 1
			}
			if v > 0 {
				incArr = append(incArr, GIDCount{pair.gid, v})
			} else if v < 0 {
				dec[pair.gid] = -v
			}
		}
		for gid, count := range countsMap {
			_, ok := groups[gid]
			if !ok {
				dec[gid] = count
			}
		}
		idx := len(countsArr)
		for gid := range groups {
			_, ok := countsMap[gid]
			if !ok {
				v := avg
				if idx < mod {
					v += 1
				}
				incArr = append(incArr, GIDCount{gid, v})
				idx += 1
			}
		}

		idx = 0
		for i, gid := range shards {
			v := gid
			_, ok := dec[gid]
			if ok {
				dec[gid] -= 1
				if dec[gid] == 0 {
					delete(dec, gid)
				}
				incArr[idx].count -= 1
				if incArr[idx].count == 0 {
					idx += 1
				}
			}
			ret[i] = v
		}
	}

	return ret
}

func (sc *ShardCtrler) applyCommand(op Op) (*Config, bool) {
	if op.Op == QUERY {
		var config Config
		if op.Num < 0 || op.Num >= len(sc.configs) {
			config = sc.configs[len(sc.configs)-1]
		} else {
			config = sc.configs[op.Num]
		}
		return &config, false
	}
	ts, ok := sc.clientTable[op.ClientID]
	if ok && ts >= op.TS {
		return nil, false
	}
	sc.clientTable[op.ClientID] = op.TS

	lastConfig := sc.configs[len(sc.configs)-1]
	groups := map[int][]string{}
	for key, value := range lastConfig.Groups {
		groups[key] = value
	}
	config := Config{Num: lastConfig.Num + 1, Groups: groups}

	if op.Op == JOIN {
		for gid := range op.Servers {
			config.Groups[gid] = op.Servers[gid]
		}
		config.Shards = sc.rebalance(config.Shards, config.Groups)
	} else if op.Op == LEAVE {
		for _, gid := range op.GIDs {
			delete(config.Groups, gid)
		}
		config.Shards = sc.rebalance(config.Shards, config.Groups)
	} else if op.Op == MOVE {
		config.Shards = lastConfig.Shards
		config.Shards[op.Shard] = op.GID
	}

	sc.configs = append(sc.configs, config)
	return nil, false
}

func (sc *ShardCtrler) releaseLockTable() {
	for _, item := range sc.lockTable {
		item.ch <- channelContent{wrongLeader: true, config: nil}
	}
	sc.lockTable = map[int]lockTableItem{}
}

func (sc *ShardCtrler) getSerializedSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.clientTable)
	return w.Bytes()
}

func (sc *ShardCtrler) readPersist(data []byte) error {
	if data == nil || len(data) < 1 {
		// bootstrap without any state
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var configs []Config
	var clientTable map[string]int64

	e := d.Decode(&configs)
	if e != nil {
		return e
	}
	e = d.Decode(&clientTable)
	if e != nil {
		return e
	}
	sc.configs = configs
	sc.clientTable = clientTable
	return nil
}

func (sc *ShardCtrler) applyLoop() {
	applyIndex := 0

	term, isLeader := sc.rf.GetState()
	for {
		if sc.killed() {
			return
		}

		var msg raft.ApplyMsg

		select {
		case <-time.After(APPLYLOOP_WAKEUP * time.Millisecond):
			newTerm, newIsLeader := sc.rf.GetState()
			if newTerm != term || newIsLeader != isLeader {
				sc.mu.Lock()
				sc.releaseLockTable()
				sc.mu.Unlock()
			}
			term = newTerm
			isLeader = newIsLeader
			continue
		case msg = <-sc.applyCh:
		}

		if msg.CommandValid {
			op := msg.Command.(Op)
			sc.mu.Lock()
			config, wrongLeader := sc.applyCommand(op)
			item, ok := sc.lockTable[msg.CommandIndex]
			if ok {
				if item.id == op.ID {
					item.ch <- channelContent{wrongLeader: wrongLeader, config: config}
				} else {
					sc.releaseLockTable()
				}
			}
			delete(sc.lockTable, msg.CommandIndex)

			if sc.maxraftstate > 0 && sc.maxraftstate <= sc.persister.RaftStateSize() {
				sc.rf.Snapshot(msg.CommandIndex, sc.getSerializedSnapshot())
			}
			sc.mu.Unlock()
		} else if msg.SnapshotValid {
			sc.mu.Lock()
			if sc.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) && msg.SnapshotIndex > applyIndex {
				sc.readPersist(msg.Snapshot)
				applyIndex = msg.SnapshotIndex
			}
			sc.releaseLockTable()
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	id := uuid.New().String()
	sc.mu.Lock()
	op := Op{
		Op:       JOIN,
		ID:       id,
		ClientID: args.ClientID,
		TS:       args.TS,
		Servers:  args.Servers,
	}
	index, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	log.Printf("[c-%v] Join(servers: %v)", sc.me, args.Servers)
	ch := make(chan channelContent)
	sc.lockTable[index] = lockTableItem{ch: ch, id: id}
	sc.mu.Unlock()

	content := <-ch
	reply.Err = OK
	reply.WrongLeader = content.wrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	id := uuid.New().String()
	sc.mu.Lock()
	op := Op{
		Op:       LEAVE,
		ID:       id,
		ClientID: args.ClientID,
		TS:       args.TS,
		GIDs:     args.GIDs,
	}
	index, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	log.Printf("[c-%v] Leave(gids: %v)", sc.me, args.GIDs)
	ch := make(chan channelContent)
	sc.lockTable[index] = lockTableItem{ch: ch, id: id}
	sc.mu.Unlock()

	content := <-ch
	reply.Err = OK
	reply.WrongLeader = content.wrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	id := uuid.New().String()
	sc.mu.Lock()
	op := Op{
		Op:       MOVE,
		ID:       id,
		ClientID: args.ClientID,
		TS:       args.TS,
		Shard:    args.Shard,
		GID:      args.GID,
	}
	index, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	log.Printf("[c-%v] Move(shard: %v, gid: %v)", sc.me, args.Shard, args.GID)
	ch := make(chan channelContent)
	sc.lockTable[index] = lockTableItem{ch: ch, id: id}
	sc.mu.Unlock()

	content := <-ch
	reply.Err = OK
	reply.WrongLeader = content.wrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	id := uuid.New().String()
	sc.mu.Lock()
	op := Op{
		Op:  QUERY,
		ID:  id,
		Num: args.Num,
	}
	index, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	log.Printf("[c-%v] Query(num: %v)", sc.me, args.Num)
	ch := make(chan channelContent)
	sc.lockTable[index] = lockTableItem{ch: ch, id: id}
	sc.mu.Unlock()

	content := <-ch
	reply.Err = OK
	reply.WrongLeader = content.wrongLeader
	if !content.wrongLeader {
		reply.Config = *content.config
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Num = 0
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.persister = persister
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.lockTable = map[int]lockTableItem{}
	sc.clientTable = map[string]int64{}
	sc.maxraftstate = 1 << 10

	e := sc.readPersist(persister.ReadSnapshot())
	if e != nil {
		log.Fatalf("%v", e)
	}

	go sc.applyLoop()

	return sc
}
