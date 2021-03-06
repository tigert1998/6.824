package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Remaining = 0
	Map       = 0
	Ongoing   = 1
	Reduce    = 1
	Finished  = 2
)

type Phase int32

func (p Phase) String() string {
	if p == Map {
		return "Map"
	} else if p == Reduce {
		return "Reduce"
	} else if p == Finished {
		return "Finished"
	} else {
		return "Undefined"
	}
}

type MapReduceConfig struct {
	nMap                  int32
	nReduce               int32
	workerMaxLostDuration time.Duration
}

type Master struct {
	config MapReduceConfig

	mtx sync.RWMutex

	phase Phase

	mapTaskManager    taskManager
	reduceTaskManager taskManager

	outputs []string
}

func (m *Master) storePhase(to int32) {
	atomic.StoreInt32((*int32)(&m.phase), to)
}

func (m *Master) getPhase() Phase {
	return Phase(atomic.LoadInt32((*int32)(&m.phase)))
}

func (m *Master) logPhase() {
	log.Printf("%v phase started", m.getPhase())
}

func (m *Master) initializeMaster(filePaths []string, nReduce int32) {
	m.config = MapReduceConfig{
		nMap:                  int32(len(filePaths)),
		nReduce:               nReduce,
		workerMaxLostDuration: 5 * time.Second,
	}
	m.phase = Map

	m.mapTaskManager.initialize(Map, int32(len(filePaths)))
	m.reduceTaskManager.initialize(Reduce, nReduce)

	m.outputs = make([]string, nReduce)
	for i := 0; i < len(filePaths); i++ {
		m.mapTaskManager.task[i].UpdateContent(&filePaths[i])
	}

	m.logPhase()
}

func (m *Master) InitializeWorker(args *struct{}, reply *InitializeWorkerReply) error {
	reply.NReduce = m.config.nReduce
	return nil
}

func (m *Master) AskForMapTask(args *struct{}, reply *AskForMapTaskReply) error {
	currentPhase := m.getPhase()

	reply.MapPhaseFinished = currentPhase > Map
	reply.Task = nil
	if currentPhase == Map {
		task := m.mapTaskManager.allocateTask(&m.mtx)
		if task != nil {
			reply.Task = task.(*MapTask)
		}
	}
	return nil
}

func (m *Master) FinishMapTask(args *FinishMapTaskArgs, reply *struct{}) error {
	if m.getPhase() != Map {
		return nil
	}

	m.mapTaskManager.finishTask(&m.mtx, args.MapID, func() {
		for key, value := range args.ReduceFilePaths {
			m.reduceTaskManager.task[key].UpdateContent(&value)
		}

		if len(m.mapTaskManager.progressMap[Finished]) == len(m.mapTaskManager.task) {
			m.storePhase(Reduce)
			m.logPhase()
		}
	})

	return nil
}

func (m *Master) AskForReduceTask(args *struct{}, reply *AskForReduceTaskReply) error {
	currentPhase := m.getPhase()

	reply.ReducePhaseFinished = currentPhase > Reduce
	reply.Task = nil
	if currentPhase == Reduce {
		task := m.reduceTaskManager.allocateTask(&m.mtx)
		if task != nil {
			reply.Task = task.(*ReduceTask)
		}
	}
	return nil
}

func (m *Master) FinishReduceTask(args *FinishReduceTaskArgs, reply *struct{}) error {
	if m.getPhase() != Reduce {
		return nil
	}

	m.reduceTaskManager.finishTask(&m.mtx, args.ReduceID, func() {
		m.outputs[args.ReduceID] = args.FilePath

		if len(m.reduceTaskManager.progressMap[Finished]) == len(m.reduceTaskManager.task) {
			for i := 0; int32(i) < m.config.nReduce; i++ {
				newFilePath := fmt.Sprintf("mr-out-%v", i)
				os.Rename(m.outputs[i], newFilePath)
				m.outputs[i] = newFilePath
			}
			m.storePhase(Finished)
			m.logPhase()
		}
	})

	return nil
}

func (m *Master) Ping(args *PingArgs, reply *struct{}) error {
	if m.getPhase() != args.Phase {
		return nil
	}

	if args.Phase == Map {
		m.mapTaskManager.lastPing[args.ID].Store(time.Now())
	} else if args.Phase == Reduce {
		m.reduceTaskManager.lastPing[args.ID].Store(time.Now())
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

func (m *Master) checkPing() {
	for !m.Done() {
		switch m.getPhase() {
		case Map:
			m.mapTaskManager.checkAlive(&m.mtx, m.config.workerMaxLostDuration)
		case Reduce:
			m.reduceTaskManager.checkAlive(&m.mtx, m.config.workerMaxLostDuration)
		}
		time.Sleep(time.Second)
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.getPhase() == Finished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(filePaths []string, nReduce int) *Master {
	m := Master{}
	m.initializeMaster(filePaths, int32(nReduce))

	go m.checkPing()
	m.server()
	return &m
}
