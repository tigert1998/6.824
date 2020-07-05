package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type WorkerStateManager struct {
	nReduce int32
	pid     int

	mtx   sync.RWMutex
	phase Phase
	id    int32
}

func (manager *WorkerStateManager) I() string {
	return fmt.Sprintf("Worker (%v)", manager.pid)
}

func (manager *WorkerStateManager) setWorkContent(phase Phase, id int32) {
	manager.mtx.Lock()
	defer manager.mtx.Unlock()

	manager.phase = phase
	manager.id = id
}

func (manager *WorkerStateManager) ping() {
	for {
		manager.mtx.RLock()
		args := PingArgs{Phase: manager.phase, ID: manager.id}
		manager.mtx.RUnlock()
		if Map <= args.Phase && args.Phase <= Reduce {
			for i := 0; i < 3 && !call("Coordinator.Ping", &args, &(struct{}{})); i++ {
				time.Sleep(200 * time.Millisecond)
			}
		} else if args.Phase == Finished {
			return
		}
		time.Sleep(time.Second)
	}
}

var global WorkerStateManager

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int32(h.Sum32() & 0x7fffffff)
}

func executeMap(task MapTask, mapf func(string, string) []KeyValue) map[int32]string {
	file, err := os.Open(task.FilePath)
	if err != nil {
		log.Fatalf("%v cannot open %v", global.I(), task.FilePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("%v cannot read %v", global.I(), task.FilePath)
	}
	file.Close()
	kva := mapf(task.FilePath, string(content))

	encoders := make([]*json.Encoder, global.nReduce)
	files := make([]*os.File, global.nReduce)
	filePaths := make([]*string, global.nReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % global.nReduce
		if filePaths[idx] == nil {
			filePaths[idx] = new(string)
			*filePaths[idx] = fmt.Sprintf("mr-%v-%v-%v", global.pid, task.ID, idx)
			files[idx], err = os.Create(*filePaths[idx])
			if err != nil {
				log.Fatalf("%v cannot create %v", global.I(), *filePaths[idx])
			}
			encoders[idx] = json.NewEncoder(files[idx])
		}

		encoders[idx].Encode(&kv)
	}

	ret := make(map[int32]string)
	for i := 0; i < int(global.nReduce); i++ {
		if files[i] != nil {
			files[i].Close()
			ret[int32(i)] = *filePaths[i]
		}
	}

	return ret
}

func executeReduce(task ReduceTask, reducef func(string, []string) string) string {
	var kva ByKey
	for _, filePath := range task.FilePaths {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("%v cannot open %v", global.I(), filePath)
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	sort.Sort(kva)

	filePath := fmt.Sprintf("mr-out-%v-%v", global.pid, task.ID)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("%v cannot create %v", global.I(), filePath)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		file.WriteString(fmt.Sprintf("%v %v\n", kva[i].Key, output))

		i = j
	}

	file.Close()

	return filePath
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	global.phase = -1

	go global.ping()

	emptyStruct := struct{}{}

	reply := InitializeWorkerReply{}
	for !call("Coordinator.InitializeWorker", &emptyStruct, &reply) {
		time.Sleep(100 * time.Millisecond)
	}
	global.nReduce = reply.NReduce
	global.pid = os.Getpid()

	for {
		reply := AskForMapTaskReply{}
		if call("Coordinator.AskForMapTask", &emptyStruct, &reply) {
			if reply.MapPhaseFinished {
				break
			} else if reply.Task != nil {
				global.setWorkContent(Map, reply.Task.ID)

				args := FinishMapTaskArgs{
					MapID:           reply.Task.ID,
					ReduceFilePaths: executeMap(*reply.Task, mapf),
				}

				for i := 0; i < 10 && !call("Coordinator.FinishMapTask", &args, &emptyStruct); i++ {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	for {
		reply := AskForReduceTaskReply{}
		if call("Coordinator.AskForReduceTask", &emptyStruct, &reply) {
			if reply.ReducePhaseFinished {
				break
			} else if reply.Task != nil {
				global.setWorkContent(Reduce, reply.Task.ID)

				args := FinishReduceTaskArgs{
					ReduceID: reply.Task.ID,
					FilePath: executeReduce(*reply.Task, reducef),
				}
				for i := 0; i < 10 && !call("Coordinator.FinishReduceTask", &args, &emptyStruct); i++ {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	global.setWorkContent(Finished, -1)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
