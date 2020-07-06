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
	"time"
)

var nReduce int32
var pid int

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

func executeMap(task MapTask, mapf func(string, string) []KeyValue) []*string {
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

	encoders := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)
	filePaths := make([]*string, nReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		if filePaths[idx] == nil {
			filePaths[idx] = new(string)
			*filePaths[idx] = fmt.Sprintf("mr-%v-%v-%v", pid, task.ID, idx)
			files[idx], err = os.Create(*filePaths[idx])
			if err != nil {
				log.Fatalf("cannot write %v", *filePaths[idx])
			}
			encoders[idx] = json.NewEncoder(files[idx])
		}

		encoders[idx].Encode(&kv)
	}

	for i := 0; i < int(nReduce); i++ {
		if files[i] != nil {
			files[i].Close()
		}
	}

	return filePaths
}

func executeReduce(task ReduceTask, reducef func(string, []string) string) string {
	var kva ByKey
	for _, filePath := range task.FilePaths {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
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

	filePath := fmt.Sprintf("mr-out-%v-%v", pid, task.ID)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("cannot create %v", filePath)
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

	reply := InitializeWorkerReply{}
	for !call("Master.InitializeWorker", &(struct{}{}), &reply) {
	}
	nReduce = reply.NReduce
	pid = os.Getpid()

	for {
		reply := AskForMapTaskReply{}
		if call("Master.AskForMapTask", &(struct{}{}), &reply) {
			if reply.MapPhaseFinished {
				break
			} else if reply.Task != nil {
				args := FinishMapTaskArgs{
					MapID:           reply.Task.ID,
					ReduceFilePaths: executeMap(*reply.Task, mapf),
				}
				for !call("Master.FinishMapTask", &args, &(struct{}{})) {
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	for {
		reply := AskForReduceTaskReply{}
		if call("Master.AskForReduceTask", &(struct{}{}), &reply) {
			if reply.ReducePhaseFinished {
				break
			} else if reply.Task != nil {
				args := FinishReduceTaskArgs{
					ReduceID: reply.Task.ID,
					FilePath: executeReduce(*reply.Task, reducef),
				}
				for !call("Master.FinishReduceTask", &args, &(struct{}{})) {
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
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
