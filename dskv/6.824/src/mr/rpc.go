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
type SetMapTaskArgs struct {
}

type SetMapTaskReply struct {
	HasRem      bool
	FileName    string
	MapNo       int
	WorkerNo    int
	ReduceNo    int
	ReduceNum   int
	HasFinished bool
}

type SetReduceTaskArgs struct {
}

type SetReduceTaskReply struct {
	HasRem      bool
	HasFinished bool
	ReduceNo    int
	WorkerNo    int
	MapNum      int
}

type RetMapTaskArgs struct {
	FileName string
	MapNo    int
	WorkerNo int
}

type RetMapTaskReply struct {
}

type RetReduceTaskArgs struct {
	ReduceNo int
	WorkerNo int
}

type RetReduceTaskReply struct {
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
