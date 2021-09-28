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
const APPLYLOOP_WAKEUP = 500

const (
	IRRELEVANT = iota
	SERVING    = iota
	SENDING    = iota
	RECEIVING  = iota
)

const (
	PUT    = iota
	APPEND = iota
	GET    = iota
)

type Op struct {
	Op       int
	Key      string
	Value    string
	ID       string
	ClientID string
	TS       int64
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

	shardsStatus [shardctrler.NShards]int32
	shardsTarget [shardctrler.NShards]int
	dic          [shardctrler.NShards]map[string]string
	lockTable    map[int]lockTableItem
	clientTable  map[string]int64
	config       shardctrler.Config
}

func (kv *ShardKV) applyCommand(op Op) (string, Err) {
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
	e.Encode(kv.dic)
	e.Encode(kv.clientTable)
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

	var dic [shardctrler.NShards]map[string]string
	var clientTable map[string]int64

	e := d.Decode(&dic)
	if e != nil {
		return e
	}
	e = d.Decode(&clientTable)
	if e != nil {
		return e
	}
	kv.dic = dic
	kv.clientTable = clientTable
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
	if kv.shardsStatus[key2shard(args.Key)] != SERVING {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	id := uuid.New().String()
	index, _, ok := kv.rf.Start(Op{Op: GET, Key: args.Key, ID: id})
	if !ok {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf("[%v] Get(key: \"%v\")", kv.me, args.Key)

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
	if kv.shardsStatus[key2shard(args.Key)] != SERVING {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
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
	log.Printf("[%v] %v(key: \"%v\", value: \"%v\")", kv.me, args.Op, args.Key, args.Value)
	ch := make(chan channelContent)
	kv.lockTable[index] = lockTableItem{ch: ch, id: id}
	kv.mu.Unlock()

	content := <-ch
	reply.Err = content.err
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
	leader := 0
	for {
		for i := leader; ; i = (i + 1) % len(kv.ctrlers) {
			if kv.killed() {
				return
			}
			args := shardctrler.QueryArgs{Num: -1}
			reply := shardctrler.QueryReply{}
			ok := kv.ctrlers[i].Call("ShardCtrler.Query", &args, &reply)
			if ok && !reply.WrongLeader {
				leader = i
				if reply.Config.Num != kv.config.Num {
					kv.mu.Lock()
					for i := 0; i < shardctrler.NShards; i++ {
						if kv.config.Shards[i] == kv.gid && reply.Config.Shards[i] != kv.gid {
							kv.shardsStatus[i] = SENDING
							kv.shardsTarget[i] = reply.Config.Shards[i]
						} else if kv.config.Shards[i] != kv.gid && reply.Config.Shards[i] == kv.gid {
							if kv.config.Shards[i] == 0 {
								kv.shardsStatus[i] = SERVING
							} else {
								kv.shardsStatus[i] = RECEIVING
								kv.shardsTarget[i] = kv.config.Shards[i]
							}
						}
					}
					kv.config = reply.Config
					kv.mu.Unlock()
				}
				break
			}
		}
		if kv.killed() {
			return
		}
		time.Sleep(QUERY_CONFIG_LOOP_INTERVAL * time.Millisecond)
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

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardsStatus[i] = IRRELEVANT
	}

	go kv.queryConfigLoop()
	go kv.applyLoop()

	return kv
}
