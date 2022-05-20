package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type TaskType int
type TaskStatus int

// TaskType Initialization
const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

// TaskStatus Intialization
const (
	NotStarted TaskStatus = iota
	Running
	Completed
	Failed
)

// TaskInfo for keeping track of task status and comm to worker through RPC
type TaskInfo struct {
	Type     TaskType
	Status   TaskStatus
	FilePath string
	WorkerId int
	TaskId   int
	NMap     int
	NReduce  int
}

type Coordinator struct {
	// Your definitions here.
	inputFiles  []string
	nReduce     int
	nReduceLeft int
	taskArray   []TaskInfo
	mu          sync.Mutex
	nMap        int
	nMapLeft    int
}

func (c *Coordinator) FindNextTask() *TaskInfo {
	if c.nMapLeft > 0 {
		for i := 0; i < c.nMap; i++ {
			if c.taskArray[i].Status == NotStarted && c.taskArray[i].Type == MapTask {
				return &c.taskArray[i]
			}
		}
		// nMapLeft decremented when task finishes, will panic when all tasks are assigned but not finished
		// panic("Cannot find available map task")
	} else if c.nReduceLeft > 0 {
		for i := c.nMap; i < len(c.taskArray); i++ {
			if c.taskArray[i].Status == NotStarted && c.taskArray[i].Type == ReduceTask {
				return &c.taskArray[i]
			}
		}
		// panic("Cannot find available reduce task")
	} else {
		// Assuming no tasks fail, return exit task when no task available
		exitTask := TaskInfo{ExitTask, NotStarted, "", -1, -1, c.nMap, c.nReduce}
		return &exitTask
	}
	noTask := TaskInfo{NoTask, NotStarted, "", -1, -1, c.nMap, c.nReduce}
	return &noTask
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *RequestTaskArgs, reply *RequestTaskResponse) error {
	workerId := args.WorkerId

	c.mu.Lock()

	newTask := c.FindNextTask()
	c.mu.Unlock()

	newTask.WorkerId = workerId
	newTask.Status = Running

	reply.Task = newTask
	// PrintTaskInfo(reply.Task)
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Status == Failed {
		c.taskArray[args.TaskId].Status = NotStarted
	} else if args.Status == Completed {
		if args.Type == MapTask {
			c.nMapLeft -= 1
		} else if args.Type == ReduceTask {
			c.nReduceLeft -= 1
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.nMapLeft == 0 && c.nReduceLeft == 0

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files
	c.nReduce = nReduce
	c.nReduceLeft = c.nReduce
	c.nMap = len(files)
	c.nMapLeft = c.nMap
	c.taskArray = make([]TaskInfo, 0, c.nMap+c.nReduce)

	log.Printf("[Coordinator] Coordinator created with %d map tasks and %d reduce tasks.\n", c.nMap, c.nReduce)

	var MakeTaskInfo func(TaskType, TaskStatus, string, int) *TaskInfo
	MakeTaskInfo = func(tt TaskType, ts TaskStatus, fp string, taskId int) *TaskInfo {
		return &TaskInfo{tt, ts, fp, -1, taskId, c.nMap, c.nReduce}
	}

	for i := 0; i < c.nMap; i++ {
		mapTask := MakeTaskInfo(MapTask, NotStarted, files[i], i)
		c.taskArray = append(c.taskArray, *mapTask)
	}

	for i := 0; i < c.nReduce; i++ {
		reduceTask := MakeTaskInfo(ReduceTask, NotStarted, strconv.Itoa(i), i+c.nMap)
		c.taskArray = append(c.taskArray, *reduceTask)
	}

	c.server()
	return &c
}
