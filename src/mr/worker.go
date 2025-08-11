package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.

	var workerID int
	rand.Seed(time.Now().UnixNano())
	workerID = rand.Int()
	for {
		args := GetTaskArgs{WorkerId: workerID}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			break
		}
		innerPrint(reply, workerID)
		switch reply.TaskType {
		case "map":
			mapping(mapf, reply, workerID)
		case "reduce":
			reducing(reducef, reply, workerID)
		default:
			time.Sleep(5 * time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func reducing(reducef func(string, []string) string, reply GetTaskReply, workerId int) {
	indexM := reply.TaskIndex
	indexR := reply.FileIndex
	outFileName := fmt.Sprintf("rout-%d-%d", indexM, indexR)

	// 创建一个切片存储要读取的所有文件内容
	var kva []KeyValue
	var mu sync.Mutex     // 用来锁住对kva的访问
	var wg sync.WaitGroup // 用来等待所有goroutine完成

	// 启动 goroutines 来并发读取文件
	for i := range reply.FileNames {
		wg.Add(1) // 每启动一个goroutine，计数加1

		go func(fileName string) {
			defer wg.Done() // 完成时减少计数

			f, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
			defer f.Close()

			var fileContent []KeyValue
			decoder := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err == io.EOF {
					break
				} else if err != nil {
					log.Fatalf("cannot decode file %v: %v", fileName, err)
				}
				fileContent = append(fileContent, kv)
			}

			// 使用锁来安全地修改kva
			mu.Lock()
			kva = append(kva, fileContent...)
			mu.Unlock()
		}(reply.FileNames[i])
	}

	// 等待所有 goroutines 完成
	wg.Wait()

	// 对所有的键值对进行排序
	intermediate := kva
	sort.Sort(ByKey(intermediate))

	// 创建输出文件
	ofile, _ := os.Create(outFileName)

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
	fmt.Printf("worker %d finished reduce task %s\n", workerId, reply.FileNames)
	finishArgs := FinishTaskArgs{TaskType: reply.TaskType, Index: reply.FileIndex, FileNames: []string{outFileName}, WorkerId: workerId}
	finishReply := FinishTaskReply{}
	call("Coordinator.FinishTask", &finishArgs, &finishReply)
}

func mapping(mapf func(string, string) []KeyValue, reply GetTaskReply, workerId int) {
	fileName := reply.FileNames[0]
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	var outputFileNames []string
	outputFileNamesSet := make(map[string]struct{})

	kva := mapf(fileName, string(content))

	// 处理生成的 key-value 对
	index_m := reply.TaskIndex
	groupedKva := make(map[int][]KeyValue)

	// 根据index_r将kva按组分组
	for _, kv := range kva {
		index_r := ihash(kv.Key) % reply.NReduce
		groupedKva[index_r] = append(groupedKva[index_r], kv)
	}

	// 按组写入文件
	for index_r, kvGroup := range groupedKva {
		outputFileName := fmt.Sprintf("mout-%d-%d", index_m, index_r)

		// 打开文件并使用 JSON 编码器写入 kvGroup 中的 key-value 对
		file, err := os.OpenFile(outputFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot open or create file %v", outputFileName)
		}

		// 创建 JSON 编码器
		enc := json.NewEncoder(file)

		// 使用 JSON 编码器写入 kvGroup 中的每个 key-value 对
		for _, kv := range kvGroup {
			if kv.Key == "ad" {
				println("error word 'ad' in file: " + reply.FileNames[0])
			}
			err := enc.Encode(&kv) // 将 key-value 对编码成 JSON 格式并写入文件
			if err != nil {
				log.Fatalf("cannot write to file %v", outputFileName)
			}
		}
		file.Close()

		if _, exists := outputFileNamesSet[outputFileName]; !exists {
			outputFileNamesSet[outputFileName] = struct{}{}
			outputFileNames = append(outputFileNames, outputFileName)
		}
	}

	// 任务完成后，通知 coordinator
	fmt.Printf("worker %d finished map task %s\n", workerId, reply.FileNames[0])
	finishArgs := FinishTaskArgs{TaskType: reply.TaskType, Index: reply.FileIndex, FileNames: outputFileNames, WorkerId: workerId}
	finishReply := FinishTaskReply{}
	call("Coordinator.FinishTask", &finishArgs, &finishReply)
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

func innerPrint(reply GetTaskReply, workerId int) {
	var sb []string
	sb = append(sb, fmt.Sprintf("goroutineId:%d", workerId))
	sb = append(sb, fmt.Sprintf("TaskType:%s", reply.TaskType))
	sb = append(sb, fmt.Sprintf("NReduce:%d", reply.NReduce))
	sb = append(sb, fmt.Sprintf("FileIndex:%d", reply.FileIndex))
	sb = append(sb, fmt.Sprintf("TaskIndex:%d", reply.TaskIndex))
	sb = append(sb, fmt.Sprintf("FileNames:%v", reply.FileNames))

	// 将所有键值对以逗号分隔并打印
	fmt.Println(strings.Join(sb, ", "))
}
