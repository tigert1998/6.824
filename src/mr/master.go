package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Map Task Progress
const (
	Remaining int32 = iota
	Ongoing
	Finished
)

const (
	Map int32 = iota
	Reduce
)

type MapTask struct {
	ID       int32
	FilePath string
}

type mapTaskState struct {
	task      MapTask
	progress  int32
	startTime *time.Time
}

type Master struct {
	mtx sync.RWMutex

	phase int32

	mapTaskStates   []mapTaskState
	mapTaskProgress [3]map[int32]struct{}

	reduceFilePaths [][]string
}

func (m *Master) initMaster(filePaths []string, nReduce int) {
	m.phase = Map
	m.mapTaskStates = make([]mapTaskState, len(filePaths))
	for i := 0; i < 3; i++ {
		m.mapTaskProgress[i] = make(map[int32]struct{})
	}

	for i, filePath := range filePaths {
		m.mapTaskStates[i].task.ID = int32(i)
		m.mapTaskStates[i].task.FilePath = filePath
		m.mapTaskStates[i].progress = Remaining
		m.mapTaskProgress[Remaining][int32(i)] = struct{}{}
	}
	m.reduceFilePaths = make([][]string, nReduce)
}

func (m *Master) allocateMapTask() *MapTask {
	m.mtx.RLock()
	if len(m.mapTaskProgress[Remaining]) == 0 {
		defer m.mtx.RUnlock()
		return nil
	} else {
		m.mtx.RUnlock()
		m.mtx.Lock()
		defer m.mtx.Unlock()

		var key int32
		for key = range m.mapTaskProgress[Remaining] {
			break
		}
		delete(m.mapTaskProgress[Remaining], key)
		m.mapTaskProgress[Ongoing][key] = struct{}{}
		m.mapTaskStates[key].progress = Ongoing
		m.mapTaskStates[key].startTime = new(time.Time)
		*m.mapTaskStates[key].startTime = time.Now()
		return &m.mapTaskStates[key].task
	}
}

func (m *Master) InitializeWorker(args *struct{}, reply *InitializeWorkerReply) error {
	reply.NReduce = int32(len(m.reduceFilePaths))
	return nil
}

func (m *Master) AskForMapTask(args *struct{}, reply *AskForMapTaskReply) error {
	reply.MapPhaseFinished = atomic.LoadInt32(&m.phase) == Reduce
	reply.Task = m.allocateMapTask()
	return nil
}

func (m *Master) FinishMapTask(args *FinishMapTaskArgs, reply *struct{}) error {
	m.mtx.RLock()
	if m.mapTaskStates[args.MapID].progress != Ongoing {
		m.mtx.RUnlock()
		return nil
	} else {
		m.mtx.RUnlock()
		m.mtx.Lock()

		delete(m.mapTaskProgress[Ongoing], args.MapID)
		m.mapTaskProgress[Finished][args.MapID] = struct{}{}
		m.mapTaskStates[args.MapID].progress = Finished

		for key, value := range args.ReduceFilePaths {
			m.reduceFilePaths[key] = append(m.reduceFilePaths[key], *value)
		}

		m.mtx.Unlock()

		if len(m.mapTaskProgress[Finished]) == len(m.mapTaskStates) {
			atomic.StoreInt32(&m.phase, Reduce)
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(filePaths []string, nReduce int) *Master {
	m := Master{}
	m.initMaster(filePaths, nReduce)

	m.server()
	return &m
}
