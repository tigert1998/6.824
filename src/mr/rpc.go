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

type InitializeWorkerReply struct {
	NReduce int32
}

type AskForMapTaskReply struct {
	MapPhaseFinished bool
	Task             *MapTask
}

type FinishMapTaskArgs struct {
	MapID           int32
	ReduceFilePaths []*string
}

type AskForReduceTaskReply struct {
	ReducePhaseFinished bool
	Task                *ReduceTask
}

type FinishReduceTaskArgs struct {
	ReduceID int32
	FilePath string
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
