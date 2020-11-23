package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MapTask struct {
	FileName    string
	MapIndex    int
	NumOfReduce int
}

type ReduceTask struct {
	NumOfMap    int // reduce需要知道有多少个map，以收集数据
	ReduceIndex int
}

type Task struct {
	Phase      string // map任务或reduce任务
	MapTask    MapTask
	ReduceTask ReduceTask
}

const (
	TASK_PHASE_MAP    = "map"
	TASK_PHASE_REDUCE = "reduce"
)

// Add your RPC definitions here.

type AskForTaskArgs struct {
	DoneTask Task // 上一次执行的任务，用于让master知道任务已执行完
}

type AskForTaskReply struct {
	IsDone bool
	Task   Task
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
