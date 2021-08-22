package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"

	"github.com/google/uuid"
)

const Debug = false

const (
	PUT    = iota
	APPEND = iota
	GET    = iota
)

const APPLYLOOP_WAKEUP = 500

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       int
	Key      string
	Value    string
	ID       string
	ClientID string
	TS       int64
}

type lockTableItem struct {
	ch chan bool
	id string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dic         map[string]string
	lockTable   map[int]lockTableItem
	clientTable map[string]int64
}

func (kv *KVServer) applyCommand(op Op) {
	if op.Op == GET {
		return
	}
	ts, ok := kv.clientTable[op.ClientID]
	if ok && ts >= op.TS {
		return
	}
	kv.clientTable[op.ClientID] = op.TS
	if op.Op == PUT {
		kv.dic[op.Key] = op.Value
	} else if op.Op == APPEND {
		var buffer bytes.Buffer
		value, ok := kv.dic[op.Key]
		if ok {
			buffer.WriteString(value)
		}
		buffer.WriteString(op.Value)
		kv.dic[op.Key] = buffer.String()
	}
}

func (kv *KVServer) releaseLockTable() {
	for _, item := range kv.lockTable {
		item.ch <- false
	}
	kv.lockTable = map[int]lockTableItem{}
}

func (kv *KVServer) applyLoop() {
	term, isLeader := kv.rf.GetState()
	for {
		if kv.killed() {
			return
		}

		var msg raft.ApplyMsg

		select {
		case <-time.After(APPLYLOOP_WAKEUP * time.Millisecond):
			newTerm, newIsLeader := kv.rf.GetState()
			if isLeader {
				if newTerm != term || !newIsLeader {
					kv.mu.Lock()
					kv.releaseLockTable()
					kv.mu.Unlock()
				}
			}
			term = newTerm
			isLeader = newIsLeader
			continue
		case msg = <-kv.applyCh:
		}

		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()
			kv.applyCommand(op)
			item, ok := kv.lockTable[msg.CommandIndex]
			if ok {
				if item.id == op.ID {
					item.ch <- true
				} else {
					kv.releaseLockTable()
				}
			}
			delete(kv.lockTable, msg.CommandIndex)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	id := uuid.New().String()
	index, _, ok := kv.rf.Start(Op{Op: GET, Key: args.Key, ID: id})
	if !ok {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf("[%v] Get(key: \"%v\")", kv.me, args.Key)

	ch := make(chan bool)
	kv.lockTable[index] = lockTableItem{ch: ch, id: id}
	kv.mu.Unlock()

	if <-ch {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
		return
	}

	var value string
	kv.mu.Lock()
	value, ok = kv.dic[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	log.Printf("[%v] %v(key: \"%v\", value: \"%v\")", kv.me, args.Op, args.Key, args.Value)
	ch := make(chan bool)
	kv.lockTable[index] = lockTableItem{ch: ch, id: id}
	kv.mu.Unlock()

	if <-ch {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0

	log.Printf("[%v] kvserver reboots!", me)

	kv.dic = map[string]string{}
	kv.lockTable = map[int]lockTableItem{}
	kv.clientTable = map[string]int64{}

	go kv.applyLoop()

	return kv
}
