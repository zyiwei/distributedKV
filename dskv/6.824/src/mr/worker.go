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
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	counter := 0
	// 向coordinator发送RPC，申请获得一个task--尚未处理的文件名
	for {
		setMapTaskArgs := SetMapTaskArgs{}
		setMapTaskReply := SetMapTaskReply{}
		counter = 0
		for {
			counter++
			ok := call("Coordinator.SetMapTask", &setMapTaskArgs, &setMapTaskReply)
			if ok {
				break
			}
			// else {
			// 	time.Sleep(1 * time.Second)
			// }
			// if counter == 10 {
			// 	return
			// }
		}
		if !setMapTaskReply.HasRem && !setMapTaskReply.HasFinished {
			time.Sleep(time.Second * 1)
		} else {
			if setMapTaskReply.HasFinished {
				break
			}
			if setMapTaskReply.HasRem {
				// 读尚未处理的文件，文件名由RPC返回
				file, err := os.Open(setMapTaskReply.FileName)
				if err != nil {
					log.Fatalf("cannot open %v", setMapTaskReply.FileName)
				}
				content, err := ioutil.ReadAll(file) // content -> 文件内容
				if err != nil {
					log.Fatalf("cannot read %v", setMapTaskReply.FileName)
				}
				file.Close()

				// map处理文件
				kva := mapf(setMapTaskReply.FileName, string(content)) // kva -> map生成的{key, value}数组
				sort.Sort(ByKey(kva))
				// 在本地创建N个文件
				interFiles := createNLocalFiles(&setMapTaskReply)

				//time.Sleep(time.Second * 2)
				// 将kva写入文件
				inputKv2Json(&kva, &interFiles, &setMapTaskReply)

				//向coordinator发送RPC，将已由map任务成功处理的文件状态置2
				retMapTaskArgs := RetMapTaskArgs{}
				retMapTaskReply := RetMapTaskReply{}
				retMapTaskArgs.FileName = setMapTaskReply.FileName
				retMapTaskArgs.MapNo = setMapTaskReply.MapNo
				retMapTaskArgs.WorkerNo = setMapTaskReply.WorkerNo

				counter = 0
				for {
					counter++
					ok := call("Coordinator.RetMapTask", &retMapTaskArgs, &retMapTaskReply)
					if ok {
						break
					}
				}
			}
		}
	}

	//fmt.Println("Map Task Finished!")

	//return

	// map任务全部完成
	for {
		counter = 0
		setReduceTaskArgs := SetReduceTaskArgs{}
		setReduceTaskReply := SetReduceTaskReply{}
		for {
			counter++
			ok := call("Coordinator.SetReduceTask", &setReduceTaskArgs, &setReduceTaskReply)
			if !ok {
				time.Sleep(1 * time.Second)
			} else {
				break
			}
			// else {
			// 	break
			// }
			// if counter == 10 {
			// 	return
			// }
		}
		if setReduceTaskReply.HasRem || setReduceTaskReply.HasFinished {
			if setReduceTaskReply.HasRem {
				kva := []KeyValue{}
				for mapNo := 0; mapNo < setReduceTaskReply.MapNum; mapNo++ {
					fileName := "mr-" + strconv.Itoa(mapNo) + "-" + strconv.Itoa(setReduceTaskReply.ReduceNo)
					file, err := os.Open(fileName)
					if err != nil {
						log.Fatalf("cannot open %v", fileName)
					}
					// 读文件，将json转成数组存储
					outputJson2Kv(&kva, file)
					file.Close()
				}
				sort.Sort(ByKey(kva))

				//time.Sleep(time.Second * 1)
				// reduce任务输出
				createOutFile(&kva, &setReduceTaskReply, reducef)

				retReduceTaskArgs := RetReduceTaskArgs{}
				retReduceTaskReply := RetReduceTaskReply{}
				retReduceTaskArgs.ReduceNo = setReduceTaskReply.ReduceNo
				retReduceTaskArgs.WorkerNo = setReduceTaskReply.WorkerNo
				counter = 0
				for {
					counter++
					ok := call("Coordinator.RetReduceTask", &retReduceTaskArgs, &retReduceTaskReply)
					if ok {
						break
					}
					// else {
					// 	time.Sleep(time.Second)
					// }
					// if counter == 10 {
					// 	return
					// }
				}

			} else {
				break
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}

}

// 在本地创建N个文件
func createNLocalFiles(reply *SetMapTaskReply) []*os.File {
	interFileName := []string{} // interFileName[] -> 本地中间文件名数组
	for i := 0; i < reply.ReduceNum; i++ {
		interFileName = append(interFileName, "mr-"+strconv.Itoa(reply.MapNo)+"-"+strconv.Itoa(i))
	}
	interFile := []*os.File{} // interFile[] -> 本地中间文件数组
	for _, fName := range interFileName {
		interOutFile, err := os.Create(fName)
		if err != nil {
			log.Fatalf("cannot create %v", fName)
		}
		interFile = append(interFile, interOutFile)
	}
	return interFile
}

// 将 KeyValue[]数组以json格式写入本地文件列表
func inputKv2Json(kva *[]KeyValue, interFile *[]*os.File, reply *SetMapTaskReply) {
	for _, kv := range *kva {
		index := ihash(kv.Key) % reply.ReduceNum
		enc := json.NewEncoder((*interFile)[index])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}
}

// 创建reduce任务的输出文件
func createOutFile(kva *[]KeyValue, reply *SetReduceTaskReply, reducef func(string, []string) string) {
	oname := "mr-out-" + strconv.Itoa(reply.ReduceNo)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(*kva) {
		j := i + 1
		for j < len(*kva) && (*kva)[j].Key == (*kva)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (*kva)[k].Value)
		}
		output := reducef((*kva)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", (*kva)[i].Key, output)

		i = j
	}
	ofile.Close()
}

// 将文件内容读入KeyValue[]数组
func outputJson2Kv(kva *[]KeyValue, file *os.File) {
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		*kva = append(*kva, kv)
	}
}

//<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
