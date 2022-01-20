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

type Coordinator struct {
	// Your definitions here.
	workerNo int

	filesMap map[string]int // 文件状态映射表 -> map[文件名]状态号
	// 0:待分配 1:map处理中 2:已处理
	fileMapLock sync.Mutex // 文件状态表锁

	file2WorkerNo map[string]int // 文件与worker映射表

	mapChan []chan bool // map任务计时锁

	file2NoMap map[string]int // 文件与map任务号映射表 -> map[文件名]任务号

	reduceMap map[int]int // reduce任务状态映射表
	// 0:待分配 1:reduce处理中 2:已处理
	reduceMapLock sync.Mutex // reduce任务状态表锁

	reduce2WorkerNo map[int]int // reduce任务号与worker号映射表

	reduceChan []chan bool // reduce任务计时锁

}

// Your code here -- RPC handlers for the worker to call.

// 为worker分配map任务
func (c *Coordinator) SetMapTask(args *SetMapTaskArgs, reply *SetMapTaskReply) error {
	c.fileMapLock.Lock()
	// defer c.fileMapLock.Unlock()

	c.reduceMapLock.Lock()
	reply.ReduceNum = len(c.reduceMap)
	c.reduceMapLock.Unlock()
	reply.HasFinished = true
	reply.HasRem = false
	for key, val := range c.filesMap {
		if val == 0 {
			reply.HasRem = true
			reply.FileName = key
			reply.MapNo = c.file2NoMap[key]
			reply.WorkerNo = c.workerNo
			c.workerNo++
			reply.HasFinished = false
			break
		} else if val == 1 {
			reply.HasFinished = false
		}
	}
	if !reply.HasRem || reply.HasFinished { // 剩余map任务进行中 || 文件状态全部为2（所有map任务均完成）
		c.fileMapLock.Unlock()
		return nil
	}
	c.file2WorkerNo[reply.FileName] = reply.WorkerNo // 该过程中的文件由reply.WorkerNo号worker处理
	c.filesMap[reply.FileName] = 1
	c.fileMapLock.Unlock()

	// 开启计时线程，设置超时时间为10s
	go c.mapTimeCounter(reply.FileName, reply.MapNo)
	return nil
}

// 计时线程
func (c *Coordinator) mapTimeCounter(key string, no int) {

	go func() {
		defer c.fileMapLock.Unlock()
		time.Sleep(10 * time.Second)
		c.fileMapLock.Lock()
		if c.filesMap[key] != 1 || len(c.mapChan[no]) != 0 {
			return
		}
		c.mapChan[no] <- false // 就尼玛这个鬼地方，死锁连续调了十多个小时，啊！！啊！！啊！！
		fmt.Println("[Map Early Exit]...")
	}()

	select {
	case res := <-c.mapChan[no]:
		c.fileMapLock.Lock()
		if res {
			c.filesMap[key] = 2
		} else {
			c.filesMap[key] = 0
		}
		c.fileMapLock.Unlock()
	}
}

// worker任务成功完成
func (c *Coordinator) RetMapTask(args *RetMapTaskArgs, reply *RetMapTaskReply) error {
	c.fileMapLock.Lock()
	defer c.fileMapLock.Unlock()

	if c.file2WorkerNo[args.FileName] != args.WorkerNo {
		return nil
	}
	if c.filesMap[args.FileName] != 1 || len(c.mapChan[args.MapNo]) != 0 {
		return nil
	}
	c.mapChan[args.MapNo] <- true

	return nil
}

// 为worker分配reduce任务
func (c *Coordinator) SetReduceTask(args *SetReduceTaskArgs, reply *SetReduceTaskReply) error {
	c.reduceMapLock.Lock()

	reply.HasFinished = true
	reply.HasRem = false
	c.fileMapLock.Lock()
	reply.MapNum = len(c.filesMap)
	c.fileMapLock.Unlock()
	for key, val := range c.reduceMap {
		if val == 0 {
			reply.HasFinished = false
			reply.HasRem = true
			reply.ReduceNo = key
			reply.WorkerNo = c.workerNo
			c.workerNo++
			c.reduceMap[key] = 1
			break
		} else if val == 1 {
			reply.HasFinished = false
		}
	}
	if reply.HasFinished || !reply.HasRem {
		c.reduceMapLock.Unlock()
		return nil
	}
	c.reduce2WorkerNo[reply.ReduceNo] = reply.WorkerNo
	c.reduceMap[reply.ReduceNo] = 1
	c.reduceMapLock.Unlock()

	go c.reduceTimeCounter(reply.ReduceNo)
	return nil
}

// reduce计时线程
func (c *Coordinator) reduceTimeCounter(no int) {
	go func() {
		defer c.reduceMapLock.Unlock()
		time.Sleep(10 * time.Second)
		c.reduceMapLock.Lock()
		if c.reduceMap[no] != 1 || len(c.reduceChan[no]) != 0 {
			return
		}
		c.reduceChan[no] <- false
		fmt.Println("[Reduce Early Exit]...")
	}()

	select {
	case res := <-c.reduceChan[no]:
		c.reduceMapLock.Lock()
		if res {
			c.reduceMap[no] = 2
		} else {
			c.reduceMap[no] = 0
		}
		c.reduceMapLock.Unlock()
	}
}

// reduce任务完成
func (c *Coordinator) RetReduceTask(args *RetReduceTaskArgs, reply *RetReduceTaskReply) error {
	c.reduceMapLock.Lock()
	defer c.reduceMapLock.Unlock()

	if c.reduce2WorkerNo[args.ReduceNo] != args.WorkerNo {
		return nil
	}
	if c.reduceMap[args.ReduceNo] != 1 || len(c.reduceChan[args.ReduceNo]) != 0 {
		return nil
	}
	c.reduceChan[args.ReduceNo] <- true
	//fmt.Printf("Reduce task %v finished\n", args.ReduceNo)

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
	ret := true

	// Your code here.
	c.reduceMapLock.Lock()
	for _, value := range c.reduceMap {
		if value != 2 {
			ret = false
		}
	}
	c.reduceMapLock.Unlock()

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
	c.filesMap = make(map[string]int)
	c.file2NoMap = make(map[string]int)
	c.reduceMap = make(map[int]int)
	c.file2WorkerNo = make(map[string]int)
	c.reduce2WorkerNo = make(map[int]int)
	c.mapChan = []chan bool{}
	c.reduceChan = []chan bool{}

	c.fileMapLock.Lock()
	counter := 0
	for _, fileName := range files {
		c.filesMap[fileName] = 0
		c.file2NoMap[fileName] = counter
		c.mapChan = append(c.mapChan, make(chan bool, 1))
		counter++
	}
	c.fileMapLock.Unlock()

	// for fileName, val := range c.filesMap {
	// 	fmt.Println(fileName, " ", val)
	// }

	c.reduceMapLock.Lock()
	for i := 0; i < nReduce; i++ {
		c.reduceMap[i] = 0
		c.reduceChan = append(c.reduceChan, make(chan bool, 1))
	}
	c.reduceMapLock.Unlock()

	//*********************************************************

	c.server()
	return &c
}
