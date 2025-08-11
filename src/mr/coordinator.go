package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	mapTasks     []Task
	reduceTasks  []Task
	intermediate []string
	finalOutput  []string
	nReduce      int
	taskIndex    int
	mapMu        sync.Mutex
	reduceMu     sync.Mutex
}

type Task struct {
	state    string
	fileName string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	mapDone := true
	// 遍历mapTasks
	c.mapMu.Lock()
	for i := range c.mapTasks {
		state := c.mapTasks[i].state
		fileName := c.mapTasks[i].fileName
		if state != "done" {
			mapDone = false
		}
		if state == "idle" {
			c.mapTasks[i].state = "in_process"
			reply.TaskType = "map"
			reply.FileIndex = i
			reply.FileNames = []string{fileName}
			reply.NReduce = c.nReduce
			reply.TaskIndex = c.taskIndex
			c.taskIndex++
			fmt.Printf("worker %d is mapping files %s\n", args.WorkerId, reply.FileNames)
			go func(i int) {
				select {
				case <-time.After(1 * time.Minute):
					c.mapMu.Lock() // 锁定
					if c.mapTasks[i].state == "in_process" {
						c.mapTasks[i].state = "idle"
						fmt.Printf("Map Task %s state reset to idle due to timeout (asigned to worker %d).\n", reply.FileNames, args.WorkerId)
					}
					c.mapMu.Unlock() // 解锁
				}
			}(i)
			c.mapMu.Unlock()
			return nil
		}
	}
	c.mapMu.Unlock()
	if mapDone {
		c.reduceMu.Lock()
		for i := range c.reduceTasks {
			state := c.reduceTasks[i].state
			if state == "idle" {
				c.reduceTasks[i].state = "in_process"
				reply.TaskType = "reduce"
				reply.FileIndex = i
				suffix := strconv.Itoa(i)
				for _, filename := range c.intermediate {
					if len(filename) > len(suffix) && filename[len(filename)-len(suffix):] == suffix {
						reply.FileNames = append(reply.FileNames, filename)
					}
				}
				reply.NReduce = c.nReduce
				reply.TaskIndex = c.taskIndex
				c.taskIndex++
				fmt.Printf("worker %d is reducing files %s\n", args.WorkerId, reply.FileNames)
				go func(i int) {
					select {
					case <-time.After(1 * time.Minute):
						c.reduceMu.Lock() // 锁定
						if c.reduceTasks[i].state == "in_process" {
							c.reduceTasks[i].state = "idle"
							fmt.Printf("Reduce Task %s state reset to idle due to timeout (asigned to worker %d).\n", reply.FileNames, args.WorkerId)
						}
						c.reduceMu.Unlock() // 解锁
					}
				}(i)
				c.reduceMu.Unlock()
				return nil
			}
		}
		c.reduceMu.Unlock()
	}
	reply.TaskType = "wait"
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	taskType := args.TaskType
	fmt.Printf("worker %d finish %s task %d\n", args.WorkerId, taskType, args.Index)
	if taskType == "map" {
		fileIndex := args.Index
		c.mapMu.Lock() // 锁定
		if c.mapTasks[fileIndex].state == "in_process" {
			c.mapTasks[fileIndex].state = "done"
			c.intermediate = append(c.intermediate, args.FileNames...)
		}
		c.mapMu.Unlock() // 解锁
	}
	if taskType == "reduce" {
		fileIndex := args.Index
		c.reduceMu.Lock() // 锁定
		if c.reduceTasks[fileIndex].state == "in_process" {
			c.reduceTasks[fileIndex].state = "done"
			c.finalOutput[fileIndex] = args.FileNames[0]
		}
		c.reduceMu.Unlock() // 解锁
	}
	return nil
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	for i := range c.reduceTasks {
		if c.reduceTasks[i].state != "done" {
			ret = false
			break
		}
	}

	if ret {
		deleteFilesByPrefixes([]string{"mout"})
		for i, oldName := range c.finalOutput {
			newName := fmt.Sprintf("mr-out-%d", i)

			// 使用 os.Rename 进行重命名
			err := os.Rename(oldName, newName)
			if err != nil {
				fmt.Printf("Error renaming file %s to %s: %v\n", oldName, newName, err)
			} else {
				fmt.Printf("Successfully renamed %s to %s\n", oldName, newName)
			}
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	deleteFilesByPrefixes([]string{"mout", "mr-out"})
	c.mapTasks = []Task{}
	c.reduceTasks = []Task{}
	c.intermediate = []string{}
	c.finalOutput = []string{}
	c.nReduce = nReduce
	c.taskIndex = 0
	c.mapMu = sync.Mutex{}
	c.reduceMu = sync.Mutex{}
	for i := range files {
		c.mapTasks = append(c.mapTasks, Task{state: "idle", fileName: files[i]})
	}
	for i := 1; i <= nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{state: "idle"})
		c.finalOutput = append(c.finalOutput, "")
	}
	c.server()
	return &c
}

func deleteFilesByPrefixes(prefixes []string) {
	// 获取当前工作目录
	dir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return
	}

	var files []string
	// 对每个前缀使用 Glob 查找文件
	for _, prefix := range prefixes {
		matchedFiles, err := filepath.Glob(filepath.Join(dir, prefix+"*"))
		if err != nil {
			fmt.Println("Error finding files for prefix", prefix, ":", err)
			return
		}
		files = append(files, matchedFiles...)
	}

	// 删除找到的文件
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			fmt.Println("Error deleting file:", err)
		} else {
			fmt.Println("Deleted file:", file)
		}
	}
}
