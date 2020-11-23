package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	SCHEDULE_TASK_SUCCESS     = "success"
	SCHEDULE_TASK_NOAVAILABLE = "noavailable"
	SCHEDULE_TASK_DONE        = "done"
)

type Master struct {
	// Your definitions here.

	Mu   sync.Mutex
	Cond *sync.Cond

	Files []string // 文件列表

	NumOfMap    int
	NumOfReduce int

	NumOfDoneMapTask    int
	NumOfDoneReduceTask int

	MapIndexChan    chan int
	ReduceIndexChan chan int

	// k-v: address of map - runtime
	RunningMapTask    map[int]int64
	RunningReduceTask map[int]int64
}

// Your code here -- RPC handlers for the worker to call.

// 尝试获取task，并保存在AskForTaskReply中
func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	m.Cond.L.Lock()
	defer m.Cond.L.Unlock() // 延迟到函数退出时执行

	m.finishTask(args.DoneTask)

	for {
		task, result := m.scheduleTask()
		switch result {
		case SCHEDULE_TASK_SUCCESS:
			reply.Task = *task
			reply.IsDone = false
			return nil
		case SCHEDULE_TASK_NOAVAILABLE:
			// 需要等待
			// 返回NOAVAILABLE来Wait，直到finishTask检测到
			// 任务完成或TaskChecker发现有任务需要re-schedule
			// 时将其唤醒
			m.Cond.Wait()
		case SCHEDULE_TASK_DONE:
			reply.IsDone = true
			return nil
		}
	}

}

func (m *Master) scheduleTask() (*Task, string) {
	// 先分配map task，保证全部的map task都分配出去并完成了，才进入reduce
	now := time.Now().Unix()
	select {
	// 从channel中取出master初始化时发送的mapIndex
	case mapIndex := <-m.MapIndexChan:
		task := &Task{
			Phase: TASK_PHASE_MAP,
			MapTask: MapTask{
				FileName:    m.Files[mapIndex],
				MapIndex:    mapIndex,
				NumOfReduce: m.NumOfReduce,
			},
		}
		m.RunningMapTask[mapIndex] = now
		return task, SCHEDULE_TASK_SUCCESS
	default:
		// 任务已经分配完毕，但可能还有正在运行的任务
		if len(m.RunningMapTask) > 0 {
			return nil, SCHEDULE_TASK_NOAVAILABLE
		}
	}

	select {
	case reduceIndex := <-m.ReduceIndexChan:
		task := &Task{
			Phase: TASK_PHASE_REDUCE,
			ReduceTask: ReduceTask{
				NumOfMap:    m.NumOfMap,
				ReduceIndex: reduceIndex,
			},
		}
		m.RunningReduceTask[reduceIndex] = now
		return task, SCHEDULE_TASK_SUCCESS
	default:
		if len(m.RunningReduceTask) > 0 {
			return nil, SCHEDULE_TASK_NOAVAILABLE
		}
	}

	return nil, SCHEDULE_TASK_DONE

}

// 处理已经处理成功的任务
// NOTE: 对于worker，如果在执行任务时crash，应该不会再保存前一次的任务，也就不会回到满足finishTask中的switch判断
//       只有当这个worker因为某些原因迟迟没有完成任务（超时，master重新分给别的worker），当其最终完成后下一次请求任
//		 务时，master会发现已经有别的worker还在处理，这时会等那个还在处理任务的worker完成任务
func (m *Master) finishTask(task Task) {
	switch task.Phase {
	case TASK_PHASE_MAP:
		// 判断是否在running里面
		if _, isOk := m.RunningMapTask[task.MapTask.MapIndex]; !isOk {
			// 由于有超时重试的机制，可能该task正在被其他worker处理
			return
		}
		delete(m.RunningMapTask, task.MapTask.MapIndex)
		m.NumOfDoneMapTask += 1
		m.Cond.Broadcast()
	case TASK_PHASE_REDUCE:
		if _, isOk := m.RunningReduceTask[task.ReduceTask.ReduceIndex]; !isOk {
			// 由于有超时重试的机制，可能该task已被其他worker处理
			return
		}
		delete(m.RunningReduceTask, task.ReduceTask.ReduceIndex)
		m.NumOfDoneReduceTask += 1
		m.Cond.Broadcast()
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
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
	// ret := false

	// Your code here.

	m.Cond.L.Lock()
	defer m.Cond.L.Unlock()
	ret := m.NumOfDoneMapTask == m.NumOfMap && m.NumOfDoneReduceTask == m.NumOfReduce

	return ret
}

// 每10s检查已经发送的任务是否完成，没有的话需要重新发送给其他worker执行
func (m *Master) taskChecker() {
	// rr
	const TIMEOUT int64 = 10
	for {
		if m.Done() == true {
			return
		}
		m.Cond.L.Lock()
		needReSchedule := false
		now := time.Now().Unix()

		for mapIndex, startTime := range m.RunningMapTask {
			if startTime+TIMEOUT < now {
				delete(m.RunningMapTask, mapIndex)
				m.MapIndexChan <- mapIndex // re-schedule
				needReSchedule = true
			}
		}

		for reduceIndex, startTime := range m.RunningReduceTask {
			if startTime+TIMEOUT < now {
				delete(m.RunningReduceTask, reduceIndex)
				m.ReduceIndexChan <- reduceIndex // re-schedule
				needReSchedule = true
			}
		}
		if needReSchedule {
			m.Cond.Broadcast()
		}
		m.Cond.L.Unlock()
		time.Sleep(time.Second)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	// 根据文件的个数创建对应数量的Map任务
	m.Cond = sync.NewCond(&m.Mu)
	m.Files = files
	m.NumOfMap = len(files)
	m.NumOfReduce = nReduce
	fmt.Printf("NumOfMap:%d,NumOfReduce:%d\n", m.NumOfMap, m.NumOfReduce)

	m.MapIndexChan = make(chan int, m.NumOfMap)
	m.ReduceIndexChan = make(chan int, m.NumOfReduce)
	m.RunningMapTask = make(map[int]int64, 0)
	m.RunningReduceTask = make(map[int]int64, 0)

	for i := 0; i < m.NumOfMap; i++ {
		m.MapIndexChan <- i
	}

	for i := 0; i < m.NumOfReduce; i++ {
		m.ReduceIndexChan <- i
	}

	go m.taskChecker()

	m.server()
	return &m
}
