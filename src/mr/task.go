package mr

import (
	"sync"
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
	task        []task
	progress    []int32
	startTime   []time.Time
	progressMap progressMap
}

func (manager *taskManager) initialize(phase int32, n int32) {
	manager.task = make([]task, n)
	for i := 0; int32(i) < n; i++ {
		if phase == Map {
			manager.task[i] = &MapTask{ID: int32(i)}
		} else if phase == Reduce {
			manager.task[i] = &ReduceTask{ID: int32(i)}
		}
	}
	manager.progress = make([]int32, n)
	manager.startTime = make([]time.Time, n)
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
		manager.startTime[key] = time.Now()
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

		callback()
	}
}
