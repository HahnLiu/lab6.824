package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

	// uncomment to send the Example RPC to the master.
	// CallExample()

	// 在循环中不断地向master发送rpc请求获取任务
	args := &AskForTaskArgs{}
	for {
		reply, isOk := AskForTask(args)

		if !isOk || reply.IsDone {
			break
		}

		task := &reply.Task

		if task.Phase == TASK_PHASE_MAP {
			mapTask := task.MapTask
			Map(mapTask.FileName, mapTask.MapIndex, mapTask.NumOfReduce, mapf)
		} else if task.Phase == TASK_PHASE_REDUCE {
			reduceTask := task.ReduceTask
			Reduce(reduceTask.NumOfMap, reduceTask.ReduceIndex, reducef)
		}

		args.DoneTask = *task
	}

}

func AskForTask(args *AskForTaskArgs) (*AskForTaskReply, bool) {
	reply := &AskForTaskReply{}
	isOk := call("Master.AskForTask", args, reply)
	return reply, isOk
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Bucket []KeyValue

func Map(fileName string, mapIndex int, NumOfReduce int, mapf func(string, string) []KeyValue) {
	buckets := make([]Bucket, NumOfReduce)

	// 读取文件的所有内容
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalf("read file %v error\n", fileName)
	}

	// 执行Map运算
	kva := mapf(fileName, string(content))
	// 将结果分桶放,使得相同key值的kv对交给一个Reduce处理
	for _, item := range kva {
		index := ihash(item.Key) % NumOfReduce
		buckets[index] = append(buckets[index], item)
	}
	for reduceIndex, bucket := range buckets {
		// 创建临时文件
		file, err := ioutil.TempFile("", "map-temp")
		if err != nil {
			log.Fatalf("create temp file failed %v", err)
		}
		// 将bucket中的kv对以json格式写入临时文件
		enc := json.NewEncoder(file)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("json encode file err %v", err)
			}
		}
		// 保存文件
		newFileName := genIntermediateFileName(mapIndex, reduceIndex)
		err = os.Rename(file.Name(), newFileName)
		if err != nil {
			log.Fatalf("rename file failed %v", err)
		}
	}
}

// 默认map把文件保存到一个地方，而reduce从同一个地方读取

// 定义临时文件名
func genIntermediateFileName(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func genOutFileName(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}

// 根据reduceIndex，读取该reduce task所有的临时文件，然后执行reduce任务，保存到genoutfile()文件
func Reduce(NumOfMap int, reduceIndex int, reducef func(string, []string) string) {
	// 读取所有的临时文件
	intermediate := make([]KeyValue, 0)
	for i := 0; i < NumOfMap; i++ {
		fileName := genIntermediateFileName(i, reduceIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		dec := json.NewDecoder(file)
		for {
			// 将file中的json格式数据存储到kv中
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			// 逐个读取kv对，保存到intermediate数组中
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	// 对数据进行排序
	sort.Sort(ByKey(intermediate))
	outFileName := genOutFileName(reduceIndex)
	f, _ := ioutil.TempFile("", "reduce_temp")
	// 进行统计
	i := 0
	for i < len(intermediate) {
		// 生成(key, list())列表
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		// 将相同key值的所有value值放在同一张string数组中，其相加即为这个单词的总数
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 处理每个键值
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	f.Close()

	os.Rename(f.Name(), outFileName)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
