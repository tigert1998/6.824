package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

const (
	Map int32 = iota
	Reduce
)

type Master struct {
	mtx sync.RWMutex

	phase int32

	mapTaskManager    taskManager
	reduceTaskManager taskManager
}

func (m *Master) initializeMaster(filePaths []string, nReduce int32) {
	m.phase = Map

	m.mapTaskManager.initialize(Map, int32(len(filePaths)))
	m.reduceTaskManager.initialize(Reduce, nReduce)

	for i := 0; i < len(filePaths); i++ {
		m.mapTaskManager.task[i].UpdateContent(&filePaths[i])
	}
}

func (m *Master) InitializeWorker(args *struct{}, reply *InitializeWorkerReply) error {
	reply.NReduce = int32(len(m.reduceTaskManager.task))
	return nil
}

func (m *Master) AskForMapTask(args *struct{}, reply *AskForMapTaskReply) error {
	reply.MapPhaseFinished = atomic.LoadInt32(&m.phase) == Reduce
	task := m.mapTaskManager.allocateTask(&m.mtx)
	if task == nil {
		reply.Task = nil
	} else {
		reply.Task = task.(*MapTask)
	}
	return nil
}

func (m *Master) FinishMapTask(args *FinishMapTaskArgs, reply *struct{}) error {
	m.mtx.RLock()
	if m.mapTaskManager.progress[args.MapID] != Ongoing {
		m.mtx.RUnlock()
		return nil
	} else {
		m.mtx.RUnlock()
		m.mtx.Lock()
		defer m.mtx.Unlock()

		m.mapTaskManager.progressMap.flip(args.MapID, Ongoing, Finished)
		m.mapTaskManager.progress[args.MapID] = Finished

		for key, value := range args.ReduceFilePaths {
			m.reduceTaskManager.task[key].UpdateContent(value)
		}

		if len(m.mapTaskManager.progressMap[Finished]) == len(m.mapTaskManager.task) {
			atomic.StoreInt32(&m.phase, Reduce)
		}
	}
	return nil
}

func (m *Master) AskForReduceTask(args *struct{}, reply *AskForReduceTaskReply) error {
	if atomic.LoadInt32(&m.phase) == Reduce {
		reply.Task = m.reduceTaskManager.allocateTask(&m.mtx).(*ReduceTask)
	} else {
		reply.Task = nil
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
	ret := atomic.LoadInt32(&m.phase) == Reduce

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(filePaths []string, nReduce int) *Master {
	m := Master{}
	m.initializeMaster(filePaths, int32(nReduce))

	m.server()
	return &m
}
