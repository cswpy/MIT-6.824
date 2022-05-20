package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workerId int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
	task     *TaskInfo
}

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{workerId: os.Getpid(), mapf: mapf, reducef: reducef}
	for {
		w.requestTask()
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func PrintTaskInfo(ti *TaskInfo) {
	fmt.Printf("Task Type: %v\tStatus: %v\nWorker id: %d\tTask id: %d\nNMap: %d\tNReduce: %d\n", ti.Type, ti.Status, ti.WorkerId, ti.TaskId, ti.NMap, ti.NReduce)
}

func (w *worker) requestTask() {
	reqArg := RequestTaskArgs{w.workerId}
	reqReply := RequestTaskResponse{}
	call("Coordinator.AssignTask", &reqArg, &reqReply)
	// PrintTaskInfo(reqReply.Task)
	w.task = reqReply.Task
	// fmt.Printf("NReduce: %d\n", reqReply.Task.NReduce)
	// log.Printf("[Worker %d] Requested a task from master\n", w.task.WorkerId)
	if w.task.Type == MapTask {
		w.executeMapTask()
	} else if w.task.Type == ReduceTask {
		w.executeReduceTask()
	} else if w.task.Type == ExitTask {
		log.Printf("[Worker %d] All tasks completed, exiting...", w.workerId)
		os.Exit(0)
	} else if w.task.Type == NoTask {
		time.Sleep(200 * time.Millisecond)
	}
}

func (w *worker) handleError(err error) {
	w.task.Status = Failed
	log.Printf("[Worker %d] Error: %v", w.workerId, err)
	w.reportTask()
	// Exit the worker process
	os.Exit(1)
}

func (w *worker) reportTask() {
	args := ReportTaskArgs{w.task.Status, w.workerId, w.task.TaskId, w.task.Type}
	var reply ReportTaskReply
	call("Coordinator.ReportTask", &args, &reply)
}

func (w *worker) executeMapTask() {
	mapTask := w.task
	if mapTask.Type != MapTask {
		log.Panic("Wrong task type given.")
	}
	var outputKV []KeyValue

	log.Printf("[Worker %d] Executing map task on %s\n", mapTask.WorkerId, mapTask.FilePath)

	content, err := ioutil.ReadFile(mapTask.FilePath)
	if err != nil {
		w.handleError(err)
	}

	outputKV = w.mapf(mapTask.FilePath, string(content))

	reduceBins := make(map[int][]KeyValue)
	// Hashing into different reduce bins
	for _, KV := range outputKV {
		key := KV.Key
		hashedKey := ihash(key) % mapTask.NReduce
		if _, ok := reduceBins[hashedKey]; !ok {
			reduceBins[hashedKey] = make([]KeyValue, 0, len(outputKV)/mapTask.NReduce)
		}
		reduceBins[hashedKey] = append(reduceBins[hashedKey], KV)
	}

	var outFilePath string
	for reduceId, KVpairs := range reduceBins {
		outFilePath = fmt.Sprintf("mr-%d-%d", mapTask.TaskId, reduceId)
		file, err := os.Create(outFilePath)
		if err != nil {
			w.handleError(err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range KVpairs {
			err := enc.Encode(kv)
			if err != nil {
				w.handleError(err)
			}
		}
	}

	w.task.Status = Completed
	w.reportTask()
}

func (w *worker) executeReduceTask() {
	// Need to read in multiple files
	reduceTask := w.task
	if reduceTask.Type != ReduceTask {
		log.Panic("Wrong task type given.")
	}

	log.Printf("[Worker %d] Executing reduce task on %s", reduceTask.WorkerId, reduceTask.FilePath)

	assignedReduceNum, err := strconv.Atoi(reduceTask.FilePath)
	if err != nil {
		w.handleError(err)
	}

	kvmap := make(map[string][]string)
	for i := 0; i < reduceTask.NMap; i++ {
		filepath := fmt.Sprintf("mr-%d-%d", i, assignedReduceNum)
		// It's possible that FileDoesNotExist error occur
		file, err := os.Open(filepath)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := kvmap[kv.Key]; !ok {
				kvmap[kv.Key] = make([]string, 0, reduceTask.NMap)
			}
			kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
		}
	}

	// Iterating the kvmap to call reducef once for each key-values pair
	res := make([]string, 0)
	for key, values := range kvmap {
		keyAgg := w.reducef(key, values)
		res = append(res, fmt.Sprintf("%v %v\n", key, keyAgg))
	}

	outFile := fmt.Sprintf("mr-out-%d", assignedReduceNum)
	err = ioutil.WriteFile(outFile, []byte(strings.Join(res, "")), 0600)
	if err != nil {
		w.handleError(err)
	}
	w.task.Status = Completed
	w.reportTask()
}

func printKeyValue(kv []KeyValue) {
	for i := 0; i < len(kv); i++ {
		fmt.Printf("%s\t%s\n", kv[i].Key, kv[i].Value)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
