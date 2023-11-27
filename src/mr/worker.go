package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	log.Printf("worder-%d 上线\n", os.Getgid())
	for {
		task, ok := CallRpcHandler(RpcTypeGetTask, -1)
		if !ok {
			log.Printf("worker-%d: rpc:所有任务已完成", os.Getgid())
			break
		}
		if task.Status == StatusFin {
			continue
		}
		log.Printf("worder-%d taskId=%d 开始处理\n", os.Getgid(), task.Id)
		if task.Type == TaskTypeMap {
			handlerMap(task, mapf)
		} else {
			handlerReduce(task, reducef)
		}

		isSuccess := false
		for time.Now().Unix()-task.StartTime < 10 {
			_, isSuccess = CallRpcHandler(RpcTypeFinTask, task.Id)
			if isSuccess {
				log.Printf("worder-%d taskId=%d 处理成功\n", os.Getgid(), task.Id)
				break
			}
		}
		if !isSuccess {
			log.Printf("worder-%d taskId=%d rpc:响应过期\n", os.Getgid(), task.Id)
		}
	}
}

func handlerMap(task *Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName[0])
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	kvs := mapf(task.FileName[0], string(content))

	hashKV := make([][]KeyValue, task.ReduceNum)
	for _, kv := range kvs {
		hashKV[ihash(kv.Key)%task.ReduceNum] = append(hashKV[ihash(kv.Key)%task.ReduceNum], kv)
	}
	split := strings.Split(task.FileName[0], "/")
	fileName := split[len(split)-1]
	for i := 0; i < task.ReduceNum; i++ {
		name := "mr-tmp-" + fileName + "-" + strconv.Itoa(i)
		ifile, _ := os.Create(name)
		enc := json.NewEncoder(ifile)
		for _, kv := range hashKV[i] {
			enc.Encode(kv)
		}

		ifile.Close()
	}
}

func handlerReduce(task *Task, reducef func(string, []string) string) {
	intermediate := shuffle(task.FileName)

	oname := "mr-out-" + strconv.Itoa(task.Id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// shuffle 聚合排序
func shuffle(fileNames []string) []KeyValue {
	var intermediate []KeyValue
	for _, file := range fileNames {
		ofile, _ := os.Open(file)
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ofile.Close()
	}
	sort.Sort(ByKey(intermediate))
	return intermediate
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallRpcHandler(tye int, taskId int) (*Task, bool) {
	req := RpcRequest{
		Type:   tye,
		TaskId: taskId,
	}
	reply := RpcReply{}

	ok := call("Coordinator.RpcHandler", &req, &reply)
	return reply.Task, ok
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
