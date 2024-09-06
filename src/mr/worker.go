package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := MessageSend{}
		reply := MessageReply{}

		//分配任务
		call("Coordinator.RequestTask", &args, &reply)

		switch reply.TaskType {
		case MapTask:
			HandleMapTask(&reply, mapf)
		case ReduceTask:
			HandleReduceTask(&reply, reducef)
		case Wait:
			time.Sleep(1 * time.Second)
		case Exit:
			os.Exit(0)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// 处理MapTask
func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) {
	// open the file
	file, err := os.Open(reply.TaskFile)
	if err != nil {
		log.Fatalf("cannot open %v", reply.TaskFile)
		return
	}
	// read the file, get the content
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.TaskFile)
		return
	}
	file.Close()

	// 解析文件生成kv对
	kva := mapf(reply.TaskFile, string(content))
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	//按照keyhash，写到临时文件中
	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", reply.TaskID, r)
		ofile, err := os.CreateTemp("", oname)
		if err != nil {
			log.Fatalf("cannot create tempfile %v", oname)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			// json格式写入
			enc.Encode(kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	args := MessageSend{
		TaskID:              reply.TaskID,
		TaskCompletedStatus: MapTaskCompleted,
	}
	//上报任务
	call("Coordinator.ReportTask", &args, &MessageReply{})
}

// 加载map过程中生辰过的临时文件（仅仅加载和r（taskId）相关的
func generateFileName(r int, NMap int) []string {
	var fileName []string
	for TaskID := 0; TaskID < NMap; TaskID++ {
		fileName = append(fileName, fmt.Sprintf("mr-%v-%v", TaskID, r))
	}
	return fileName
}

// 处理ReduceTask
func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) {
	// load the intermediate files
	var intermediate []KeyValue
	// 获取map阶段生成的临时文件
	intermediateFiles := generateFileName(reply.TaskID, reply.NMap)

	//将kv对加载到内存中
	for _, filename := range intermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}
		// decode the intermediate file
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err == io.EOF {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 按照key排序kv对
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// 创建文件写入最终结果
	oname := fmt.Sprintf("mr-out-%v", reply.TaskID)
	ofile, err := os.CreateTemp("", oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
		return
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// 统计次数
		output := reducef(intermediate[i].Key, values)

		// 写入文件
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)

	// 汇报完成任务
	args := MessageSend{
		TaskID:              reply.TaskID,
		TaskCompletedStatus: ReduceTaskCompleted,
	}
	//上报任务
	call("Coordinator.ReportTask", &args, &MessageReply{})
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
