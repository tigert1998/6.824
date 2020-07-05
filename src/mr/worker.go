package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int32(h.Sum32() & 0x7fffffff)
}

func executeMap(nReduce int32, task MapTask, mapf func(string, string) []KeyValue) []*string {
	pid := os.Getpid()
	file, err := os.Open(task.FilePath)
	if err != nil {
		log.Fatalf("cannot open %v", task.FilePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FilePath)
	}
	file.Close()
	kva := mapf(task.FilePath, string(content))

	files := make([]*os.File, nReduce)
	filePaths := make([]*string, nReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		if files[idx] == nil {
			filePaths[idx] = new(string)
			*filePaths[idx] = fmt.Sprintf("mr-%v-%v-%v", pid, task.ID, idx)
			files[idx], err = os.Create(*filePaths[idx])
			if err != nil {
				log.Fatalf("cannot write %v", *filePaths[idx])
			}
		}

		files[idx].WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
	}

	for i := 0; i < int(nReduce); i++ {
		if files[i] != nil {
			files[i].Close()
		}
	}

	return filePaths
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reply := InitializeWorkerReply{}
	call("Master.InitializeWorker", &(struct{}{}), &reply)
	nReduce := reply.NReduce

	for {
		time.Sleep(500 * time.Millisecond)
		reply := AskForMapTaskReply{}
		if call("Master.AskForMapTask", &(struct{}{}), &reply) {
			if reply.MapPhaseFinished {
				break
			} else if reply.Task == nil {
				continue
			} else {
				args := FinishMapTaskArgs{
					MapID:           reply.Task.ID,
					ReduceFilePaths: executeMap(nReduce, *reply.Task, mapf),
				}
				for call("Master.FinishMapTask", &args, &(struct{}{})) == false {
				}
			}
		} else {
			continue
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
