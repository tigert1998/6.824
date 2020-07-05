package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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

type mapTask struct {
	id       int32
	filePath string
}

type mapTaskState struct {
	task      mapTask
	progress  int32
	startTime *time.Time
}

type Master struct {
	mtx sync.RWMutex

	mapTaskStates   []mapTaskState
	mapTaskProgress [3]map[int32]struct{}

	reduceFilePaths map[int32][]string
}

func (m *Master) initMaster(filePaths []string) {
	m.mapTaskStates = make([]mapTaskState, len(filePaths))
	for i, filePath := range filePaths {
		m.mapTaskStates[i].task.id = int32(i)
		m.mapTaskStates[i].task.filePath = filePath
		m.mapTaskStates[i].progress = Remaining
		m.mapTaskProgress[Remaining][int32(i)] = struct{}{}
	}
}

func (m *Master) currentPhase() int32 {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	if len(m.mapTaskProgress[Finished]) == len(m.mapTaskStates) {
		return Reduce
	} else {
		return Map
	}
}

func (m *Master) allocateMapTask() *mapTask {
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
		*m.mapTaskStates[key].startTime = time.Now()
		return &m.mapTaskStates[key].task
	}
}

func (m *Master) askForMapTask(args *interface{}, reply *askForMapTaskReply) error {
	reply.mapPhaseFinished = m.currentPhase() == Reduce
	reply.task = m.allocateMapTask()
	return nil
}

func (m *Master) finishMapTask(args *finishMapTaskArgs, reply *interface{}) error {
	m.mtx.RLock()
	if m.mapTaskStates[args.mapID].progress != Ongoing {
		m.mtx.RUnlock()
		return nil
	} else {
		m.mtx.RUnlock()
		m.mtx.Lock()
		defer m.mtx.Unlock()

		delete(m.mapTaskProgress[Ongoing], args.mapID)
		m.mapTaskProgress[Finished][args.mapID] = struct{}{}
		m.mapTaskStates[args.mapID].progress = Finished

		for key, value := range args.reduceFilePaths {
			m.reduceFilePaths[key] = append(m.reduceFilePaths[key], value...)
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
	m.initMaster(filePaths)

	m.server()
	return &m
}
