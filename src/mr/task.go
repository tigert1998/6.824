package mr

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type task interface {
	GetID() int32
	UpdateContent(*string)
}

type MapTask struct {
	ID       int32
	FilePath string
}

func (task *MapTask) GetID() int32 {
	return task.ID
}

func (task *MapTask) UpdateContent(filePath *string) {
	if filePath != nil {
		task.FilePath = *filePath
	}
}

type ReduceTask struct {
	ID        int32
	FilePaths []string
}

func (task *ReduceTask) GetID() int32 {
	return task.ID
}

func (task *ReduceTask) UpdateContent(filePath *string) {
	if filePath != nil {
		task.FilePaths = append(task.FilePaths, *filePath)
	}
}

type progressMap [3]map[int32]struct{}

func (m *progressMap) flip(idx int32, from int32, to int32) {
	delete(m[from], idx)
	m[to][idx] = struct{}{}
}

func (m *progressMap) initialize(n int32) {
	for i := 0; i < 3; i++ {
		m[i] = make(map[int32]struct{})
	}
	for i := 0; int32(i) < n; i++ {
		m[Remaining][int32(i)] = struct{}{}
	}
}

type taskManager struct {
	phase       Phase
	task        []task
	progress    []int32
	lastPing    []atomic.Value
	progressMap progressMap
}

func (manager *taskManager) initialize(phase Phase, n int32) {
	manager.phase = phase
	manager.task = make([]task, n)
	for i := 0; int32(i) < n; i++ {
		if phase == Map {
			manager.task[i] = &MapTask{ID: int32(i)}
		} else if phase == Reduce {
			manager.task[i] = &ReduceTask{ID: int32(i)}
		}
	}
	manager.progress = make([]int32, n)
	manager.lastPing = make([]atomic.Value, n)
	manager.progressMap.initialize(n)

	for i := 0; int32(i) < n; i++ {
		manager.progress[i] = Remaining
	}
}

func (manager *taskManager) allocateTask(mtx *sync.RWMutex) task {
	mtx.RLock()
	if len(manager.progressMap[Remaining]) == 0 {
		mtx.RUnlock()
		return nil
	} else {
		mtx.RUnlock()
		mtx.Lock()
		defer mtx.Unlock()

		var key int32
		for key = range manager.progressMap[Remaining] {
			break
		}
		manager.progressMap.flip(key, Remaining, Ongoing)
		manager.progress[key] = Ongoing
		manager.lastPing[key].Store(time.Now())
		return manager.task[key]
	}
}

func (manager *taskManager) finishTask(mtx *sync.RWMutex, idx int32, callback func()) {
	mtx.RLock()
	if manager.progress[idx] != Ongoing {
		mtx.RUnlock()
	} else {
		mtx.RUnlock()
		mtx.Lock()
		defer mtx.Unlock()

		if manager.progress[idx] != Ongoing {
			return
		}
		manager.progressMap.flip(idx, Ongoing, Finished)
		manager.progress[idx] = Finished

		log.Printf("%v task %v completed", manager.phase, idx)

		callback()
	}
}

func (manager *taskManager) checkAlive(mtx *sync.RWMutex, limit time.Duration) {
	mtx.Lock()
	defer mtx.Unlock()

	tp := time.Now()
	for i := 0; i < len(manager.lastPing); i++ {
		if manager.progress[i] != Ongoing {
			continue
		}
		lastPing := manager.lastPing[i].Load().(time.Time)
		if tp.Sub(lastPing) > limit {
			manager.progress[i] = Remaining
			manager.progressMap.flip(int32(i), Ongoing, Remaining)

			log.Printf("%v task %v reset", manager.phase, i)
		}
	}
}
