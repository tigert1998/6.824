package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/shardctrler"
	"github.com/google/uuid"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const QUERY_CONFIG_LOOP_INTERVAL = 100
const TRANSFER_LOOP_INTERVAL = 500
const APPLYLOOP_WAKEUP = 500

const ErrConfigNotReady = "ErrConfigNotReady"

const (
	IRRELEVANT = iota
	SERVING    = iota
	SENDING    = iota
	RECEIVING  = iota
)

const (
	PUT       = iota
	APPEND    = iota
	GET       = iota
	CONFIG    = iota
	SHARD_PUT = iota
	GC        = iota
)

func copyKVMap(dic *map[string]string) map[string]string {
	ret := map[string]string{}
	for k, v := range *dic {
		ret[k] = v
	}
	return ret
}

func copyClientTable(clientTable *map[string]int64) map[string]int64 {
	ret := map[string]int64{}
	for k, v := range *clientTable {
		ret[k] = v
	}
	return ret
}

func (kv *ShardKV) updateClientTable(clientTable *map[string]int64) {
	for k, v := range *clientTable {
		prev, ok := kv.clientTable[k]
		if ok {
			if v > prev {
				kv.clientTable[k] = v
			} else {
				kv.clientTable[k] = prev
			}
		} else {
			kv.clientTable[k] = v
		}
	}
}

type Op struct {
	Op          int
	Key         string
	Value       string
	Config      *shardctrler.Config
	ID          string
	ClientID    string
	TS          int64
	KV          map[string]string
	ClientTable map[string]int64
	Shard       int
}

type channelContent struct {
	err   Err
	value string
}

type lockTableItem struct {
	ch chan channelContent
	id string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	dead         int32
	rf           *raft.Raft
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk

	lockTable map[int]lockTableItem

	shardsStatus        [shardctrler.NShards]int32
	ownShards           [shardctrler.NShards]bool
	nextConfigReadyFlag int32
	dic                 [shardctrler.NShards]map[string]string
	clientTable         map[string]int64
	config              shardctrler.Config
}

func (kv *ShardKV) updateConfig(config shardctrler.Config) {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.ownShards[i] && config.Shards[i] != kv.gid {
			kv.shardsStatus[i] = SENDING
		} else if !kv.ownShards[i] && config.Shards[i] == kv.gid {
			if kv.config.Shards[i] == 0 {
				kv.shardsStatus[i] = SERVING
				kv.ownShards[i] = true
			} else {
				kv.shardsStatus[i] = RECEIVING
			}
		}
	}
	kv.config = config
	kv.checkNextConfigReady()
}

func (kv *ShardKV) checkNextConfigReady() {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.shardsStatus[i] == SENDING || kv.shardsStatus[i] == RECEIVING {
			atomic.StoreInt32(&kv.nextConfigReadyFlag, 0)
			return
		}
	}
	atomic.StoreInt32(&kv.nextConfigReadyFlag, 1)
}

func (kv *ShardKV) nextConfigReady() bool {
	return atomic.LoadInt32(&kv.nextConfigReadyFlag) == 1
}

func (kv *ShardKV) applyCommand(op Op) (string, Err) {
	if op.Op == CONFIG {
		if op.Config.Num == kv.config.Num+1 && kv.nextConfigReady() {
			kv.updateConfig(*op.Config)
			return "", OK
		} else {
			return "", ErrConfigNotReady
		}
	}
	if op.Op == SHARD_PUT {
		if kv.shardsStatus[op.Shard] == RECEIVING {
			kv.shardsStatus[op.Shard] = SERVING
			kv.dic[op.Shard] = copyKVMap(&op.KV)
			kv.updateClientTable(&op.ClientTable)
			kv.ownShards[op.Shard] = true

			kv.checkNextConfigReady()
		}
		return "", OK
	}
	if op.Op == GC {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shardsStatus[i] == SENDING {
				kv.dic[i] = map[string]string{}
				kv.shardsStatus[i] = IRRELEVANT
				kv.ownShards[i] = false
			}
		}
		kv.checkNextConfigReady()
		return "", OK
	}
	shard := key2shard(op.Key)
	if kv.shardsStatus[shard] != SERVING {
		return "", ErrWrongGroup
	}
	if op.Op == GET {
		return kv.dic[shard][op.Key], OK
	}
	ts, ok := kv.clientTable[op.ClientID]
	if ok && ts >= op.TS {
		return "", OK
	}
	kv.clientTable[op.ClientID] = op.TS
	if op.Op == PUT {
		kv.dic[shard][op.Key] = op.Value
	} else if op.Op == APPEND {
		var buffer bytes.Buffer
		value, ok := kv.dic[shard][op.Key]
		if ok {
			buffer.WriteString(value)
		}
		buffer.WriteString(op.Value)
		kv.dic[shard][op.Key] = buffer.String()
	}
	return "", OK
}

func (kv *ShardKV) releaseLockTable() {
	for _, item := range kv.lockTable {
		item.ch <- channelContent{err: ErrWrongLeader, value: ""}
	}
	kv.lockTable = map[int]lockTableItem{}
}

func (kv *ShardKV) getSerializedSnapshot() []byte {
	// TODO
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.shardsStatus)
	e.Encode(kv.ownShards)
	e.Encode(kv.dic)
	e.Encode(kv.clientTable)
	e.Encode(kv.config)
	return w.Bytes()
}

func (kv *ShardKV) readPersist(data []byte) error {
	// TODO
	if data == nil || len(data) < 1 {
		// bootstrap without any state
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var shardsStatus [shardctrler.NShards]int32
	var ownShards [shardctrler.NShards]bool
	var dic [shardctrler.NShards]map[string]string
	var clientTable map[string]int64
	var config shardctrler.Config

	e := d.Decode(&shardsStatus)
	if e != nil {
		return e
	}
	e = d.Decode(&ownShards)
	if e != nil {
		return e
	}
	e = d.Decode(&dic)
	if e != nil {
		return e
	}
	e = d.Decode(&clientTable)
	if e != nil {
		return e
	}
	e = d.Decode(&config)
	if e != nil {
		return e
	}
	kv.shardsStatus = shardsStatus
	kv.ownShards = ownShards
	kv.dic = dic
	kv.clientTable = clientTable
	kv.config = config
	return nil
}

func (kv *ShardKV) applyLoop() {
	applyIndex := 0

	term, isLeader := kv.rf.GetState()
	for {
		if kv.killed() {
			return
		}

		var msg raft.ApplyMsg

		select {
		case <-time.After(APPLYLOOP_WAKEUP * time.Millisecond):
			newTerm, newIsLeader := kv.rf.GetState()
			if newTerm != term || newIsLeader != isLeader {
				kv.mu.Lock()
				kv.releaseLockTable()
				kv.mu.Unlock()
			}
			term = newTerm
			isLeader = newIsLeader
			continue
		case msg = <-kv.applyCh:
		}

		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()
			applyIndex = msg.CommandIndex
			value, err := kv.applyCommand(op)
			item, ok := kv.lockTable[msg.CommandIndex]
			if ok {
				if item.id == op.ID {
					item.ch <- channelContent{err: err, value: value}
				} else {
					kv.releaseLockTable()
				}
			}
			delete(kv.lockTable, msg.CommandIndex)

			if kv.maxraftstate > 0 && kv.maxraftstate <= kv.persister.RaftStateSize() {
				kv.rf.Snapshot(msg.CommandIndex, kv.getSerializedSnapshot())
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) && msg.SnapshotIndex > applyIndex {
				kv.readPersist(msg.Snapshot)
				applyIndex = msg.SnapshotIndex
			}
			kv.releaseLockTable()
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	id := uuid.New().String()
	index, _, ok := kv.rf.Start(Op{Op: GET, Key: args.Key, ID: id})
	if !ok {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf("[%v-%v] Get(key: \"%v\")", kv.gid, kv.me, args.Key)

	ch := make(chan channelContent)
	kv.lockTable[index] = lockTableItem{ch: ch, id: id}
	kv.mu.Unlock()

	content := <-ch
	if content.err == OK {
		reply.Err = OK
		reply.Value = content.value
	} else {
		reply.Err = content.err
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op int
	if args.Op == "Append" {
		op = APPEND
	} else {
		op = PUT
	}

	kv.mu.Lock()
	ts, ok := kv.clientTable[args.ClientID]
	if ok && ts >= args.TS {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	id := uuid.New().String()
	index, _, ok := kv.rf.Start(Op{
		Op:       op,
		Key:      args.Key,
		Value:    args.Value,
		ID:       id,
		ClientID: args.ClientID,
		TS:       args.TS,
	})
	if !ok {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf("[%v-%v] %v(key: \"%v\", value: \"%v\")", kv.gid, kv.me, args.Op, args.Key, args.Value)
	ch := make(chan channelContent)
	kv.lockTable[index] = lockTableItem{ch: ch, id: id}
	kv.mu.Unlock()

	content := <-ch
	reply.Err = content.err
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	kv.mu.Lock()
	if kv.config.Num < args.Num {
		kv.mu.Unlock()
		reply.Success = false
		reply.WrongLeader = false
		return
	}
	if kv.config.Num > args.Num || kv.shardsStatus[args.Shard] == SERVING {
		kv.mu.Unlock()
		reply.Success = true
		reply.WrongLeader = false
		return
	}
	id := uuid.New().String()
	index, _, ok := kv.rf.Start(Op{
		Op:          SHARD_PUT,
		ID:          id,
		KV:          copyKVMap(&args.KV),
		ClientTable: copyClientTable(&args.ClientTable),
		Shard:       args.Shard,
	})
	if !ok {
		kv.mu.Unlock()
		reply.Success = false
		reply.WrongLeader = true
		return
	}
	log.Printf("[%v-%v] ShardPut(shard: %v)", kv.gid, kv.me, args.Shard)
	ch := make(chan channelContent)
	kv.lockTable[index] = lockTableItem{ch: ch, id: id}
	kv.mu.Unlock()

	content := <-ch
	reply.Success = content.err == OK
	reply.WrongLeader = content.err == ErrWrongLeader
}

func (kv *ShardKV) transferLoop() {
	for {
		if kv.killed() {
			return
		}
		time.Sleep(QUERY_CONFIG_LOOP_INTERVAL * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		if kv.nextConfigReady() {
			continue
		}

		sendingSet := map[int]struct{}{}
		kv.mu.Lock()
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shardsStatus[i] == SENDING {
				sendingSet[i] = struct{}{}
			}
		}
		clientTable := copyClientTable(&kv.clientTable)
		kv.mu.Unlock()

		waitGroup := sync.WaitGroup{}
		waitGroup.Add(len(sendingSet))

		for i := range sendingSet {
			go func(shard int) {
				args := TransferArgs{
					Num:         kv.config.Num,
					Shard:       shard,
					KV:          copyKVMap(&kv.dic[shard]),
					ClientTable: clientTable,
				}
				var reply TransferReply
				group := kv.config.Groups[kv.config.Shards[shard]]

				for {
					for j := 0; ; j = (j + 1) % len(group) {
						server := kv.make_end(group[j])
						ok := server.Call("ShardKV.Transfer", &args, &reply)
						if ok && reply.Success {
							waitGroup.Done()
							return
						}
					}
				}
			}(i)
		}

		waitGroup.Wait()

		kv.mu.Lock()
		id := uuid.New().String()
		index, _, ok := kv.rf.Start(Op{
			Op: GC,
			ID: id,
		})
		if !ok {
			kv.mu.Unlock()
			continue
		}
		log.Printf("[%v-%v] GC", kv.gid, kv.me)
		ch := make(chan channelContent)
		kv.lockTable[index] = lockTableItem{ch: ch, id: id}
		kv.mu.Unlock()

		<-ch
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) queryConfigLoop() {
	kv.mu.Lock()
	configNum := kv.config.Num
	kv.mu.Unlock()
	for {
		if kv.killed() {
			return
		}
		time.Sleep(QUERY_CONFIG_LOOP_INTERVAL * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		if !kv.nextConfigReady() {
			continue
		}

		config := kv.mck.Query(configNum + 1)

		if config.Num == configNum+1 {
			kv.mu.Lock()
			configNum = kv.config.Num
			if config.Num != configNum+1 {
				kv.mu.Unlock()
				continue
			}
			id := uuid.New().String()
			index, _, ok := kv.rf.Start(Op{Op: CONFIG, Config: &config, ID: id})
			if !ok {
				kv.mu.Unlock()
				continue
			}
			log.Printf("[%v-%v] Config(num: %v)", kv.gid, kv.me, config.Num)

			ch := make(chan channelContent)
			kv.lockTable[index] = lockTableItem{ch: ch, id: id}
			kv.mu.Unlock()

			content := <-ch
			if content.err == OK {
				configNum += 1
			}
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clientTable = map[string]int64{}
	kv.lockTable = map[int]lockTableItem{}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.dic[i] = map[string]string{}
		kv.shardsStatus[i] = IRRELEVANT
		kv.ownShards[i] = false
	}

	e := kv.readPersist(persister.ReadSnapshot())
	if e != nil {
		log.Fatalf("%v", e)
	}
	kv.checkNextConfigReady()

	go kv.transferLoop()
	go kv.queryConfigLoop()
	go kv.applyLoop()

	return kv
}
