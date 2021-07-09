package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.
type TaskType int

// go enum substitution
const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3 //only if every Map&&Reduce tasks done ,signal for job exit
)

type GetTaskArgs struct {
}
type GetTaskReply struct {
	TaskType TaskType

	TaskNum int

	// after finish Map Task , worker will convention tempFiles into mr-X-Y files,
	// where X is the Map task number, and Y is the reduce task number.

	// needed for Map (to know which file to write, use hash(key)%NReduceTasks to choose the reduce
	// task number for each KeyValue emitted by Map.)
	NReduceTasks int

	// needed for Map(to know which origin file to read)
	MapFile string

	// needed for Reduce(to know how many intermediate map files to read)
	NMapTasks int
}

type FinishTaskArgs struct {
	TaskType TaskType
	TaskNum  int
}

type FinishTaskReply struct {
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
