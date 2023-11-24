package mr

import (
	"errors"
	"sync"
)

type TaskHolder struct {
	mu             sync.Mutex
	taskMap        map[int]*Task
	taskQueue      []int
	mapFinCount    int
	reduceFinCount int
	size           int
}

func (h *TaskHolder) SetStatusFin(taskId int) {
	task, ok := h.taskMap[taskId]
	if !ok {
		return
	}
	task.Status = StatusFin
}

func (h *TaskHolder) Size() int {
	return h.size
}

func (h *TaskHolder) FinTask(taskId int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	task, err := h.GetTask(taskId)
	if err != nil {
		return
	}
	task.Status = StatusFin
	if task.Type == TaskTypeMap {
		h.mapFinCount--
	} else {
		h.reduceFinCount--
	}
}

func (h *TaskHolder) GetTask(id int) (*Task, error) {
	task, ok := h.taskMap[id]
	if !ok {
		return nil, errors.New("task不存在")
	}
	return task, nil
}

func (h *TaskHolder) IsFinMap() bool {
	return h.mapFinCount <= 0
}

func (h *TaskHolder) IsFinReduce() bool {
	return h.reduceFinCount <= 0
}

func (h *TaskHolder) GetFrontTask() *Task {
	if len(h.taskQueue) == 0 {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	taskId := h.taskQueue[0]
	return h.taskMap[taskId]
}

func (h *TaskHolder) AddBackTask(task *Task) {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, ok := h.taskMap[task.Id]
	if !ok {
		h.taskMap[task.Id] = task
	}
	h.taskQueue = append(h.taskQueue, task.Id)
	h.size++
}

func (h *TaskHolder) DelFrontTask() {
	if len(h.taskQueue) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	taskId := h.taskQueue[0]
	delete(h.taskMap, taskId)
	h.taskQueue = h.taskQueue[1:]
	h.size--
}
