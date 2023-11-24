package mr

import (
	"errors"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	StatusStart = iota
	StatusMap
	StatusReduce
	StatusFin
)

const (
	TaskTypeMap = iota
	TaskTypeReduce
)

type Task struct {
	Id        int
	Type      int
	Status    int
	FileName  []string
	ReduceNum int
	StartTime int64
}

type Coordinator struct {
	mu        sync.Mutex
	TaskIdx   int
	files     []string
	ReduceNum int
	TaskChan  chan *Task
	Status    int
	Holder    TaskHolder
}

// RpcHandler worker会调用的rpc方法
func (c *Coordinator) RpcHandler(req *RpcRequest, reply *RpcReply) error {
	for c.Status != StatusFin {
		if req.Type == RpcTypeGetTask {
			select {
			case task := <-c.TaskChan:
				reply.Task = task
				task.StartTime = time.Now().Unix()
				c.Holder.AddBackTask(task) // 加入到过期队列中
				//log.Printf("收到获取任务请求，taskId=%d type=%d", task.Id, task.Type)
			case _ = <-time.After(time.Millisecond * 100):
			}
			if reply.Task != nil {
				return nil
			}
		}
		if req.Type == RpcTypeFinTask {
			c.Holder.FinTask(req.TaskId)
			//log.Printf("收到任务完成响应，taskId=%d type=%d", req.TaskId, req.Type)
			return nil
		}
	}

	return errors.New("StatusFin")
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.Holder.mapFinCount == 0 && c.Status == StatusMap {
		c.Status = StatusReduce
		//log.Printf("map阶段已经完成")
		go c.addReduceTask()
	}
	if c.Holder.reduceFinCount == 0 && c.Status == StatusReduce {
		//log.Printf("reduce阶段已经完成")
		c.Status = StatusFin
		return true
	}

	return false
}

// MakeCoordinator create a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fileCount := len(files)
	c := Coordinator{
		mu:        sync.Mutex{},
		TaskIdx:   0,
		files:     files,
		ReduceNum: nReduce,
		TaskChan:  make(chan *Task, fileCount+nReduce),
		Status:    StatusStart,
		Holder: TaskHolder{
			taskMap:        make(map[int]*Task),
			taskQueue:      make([]int, 0),
			mapFinCount:    fileCount,
			reduceFinCount: nReduce,
		},
	}

	c.Status = StatusMap
	go c.addMapTask()
	go c.judgeTaskStatus()

	c.server()

	//log.Printf("coordinator已上线\n")

	return &c
}

func (c *Coordinator) judgeTaskStatus() {
	for c.Status != StatusFin {
		if c.Holder.Size() != 0 {
			task := c.Holder.GetFrontTask()
			if task == nil {
				continue
			}
			if task.Status == StatusFin {
				c.Holder.DelFrontTask()
				//log.Printf("task执行成功 type=%v taskId=%v\n", task.Type, task.Id)
				continue
			}
			if time.Now().Unix()-task.StartTime > 10 {
				c.Holder.DelFrontTask()
				c.addTask(task)
				//log.Printf("task过期 type=%v taskId=%v\n", task.Type, task.Id)
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func (c *Coordinator) getTaskId() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.TaskIdx++
	return c.TaskIdx
}

func (c *Coordinator) addTask(task *Task) {
	if task == nil {
		return
	}
	c.TaskChan <- task
}

func (c *Coordinator) addMapTask() {
	fileCount := len(c.files)
	for i := 0; i < fileCount; i++ {
		c.addTask(&Task{
			Id:        c.getTaskId(),
			Type:      TaskTypeMap,
			Status:    StatusStart,
			FileName:  []string{c.files[i]},
			ReduceNum: c.ReduceNum,
			StartTime: 0,
		})
	}
}

func (c *Coordinator) addReduceTask() {
	for i := 0; i < c.ReduceNum; i++ {
		c.addTask(&Task{
			Id:        c.getTaskId(),
			Type:      TaskTypeReduce,
			Status:    StatusStart,
			FileName:  c.selectReduceFileName(i),
			ReduceNum: c.ReduceNum,
			StartTime: 0,
		})
	}
}

func (c *Coordinator) selectReduceFileName(id int) []string {
	fileName := make([]string, len(c.files))
	for i, file := range c.files {
		fileName[i] = "mr-tmp-" + file + "-" + strconv.Itoa(id)
	}
	return fileName
}
