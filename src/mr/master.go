package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mu                  sync.Mutex
	cond                *sync.Cond
	mapFiles            []string
	nMapTasks           int
	nReduceTasks        int
	mapTasksFinished    []bool
	mapTasksIssued      []time.Time
	reduceTasksFinished []bool
	reduceTasksIssued   []time.Time

	isDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// handle getTask rpc
func (m *Master) HandleGetTask(args GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reply.NMapTasks = m.nMapTasks
	reply.NReduceTasks = m.nReduceTasks
	for {
		AllMapDone := true
		for i, thisMapDone := range m.mapTasksFinished {
			if !thisMapDone {
				if m.mapTasksIssued[i].IsZero() || m.mapTasksIssued[i].Second() > 10 {
					reply.TaskType = Map
					reply.TaskNum = i
					reply.MapFile = m.mapFiles[i]
					m.mapTasksIssued[i] = time.Now()
					return nil
				} else {
					AllMapDone = false
				}
			}
		}
		if !AllMapDone {
			m.cond.Wait()
		} else {
			break
		}
	}

	for {
		AllReduceDone := true
		for i, thisReduceDone := range m.reduceTasksFinished {
			if !thisReduceDone {
				if m.reduceTasksIssued[i].IsZero() || m.reduceTasksIssued[i].Second() > 10 {
					reply.TaskType = Reduce
					reply.TaskNum = i
					m.reduceTasksIssued[i] = time.Now()
					return nil
				} else {
					AllReduceDone = false
				}
			}
		}
		if !AllReduceDone {
			m.cond.Wait()
		} else {
			break
		}
	}

	reply.TaskType = Done
	m.isDone = true

	return nil
}

func (m *Master) HandleFinishedTask(args FinishTaskArgs, reply *FinishTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.TaskType {
	case Map:
		m.mapTasksFinished[args.TaskNum] = true
	case Reduce:
		m.reduceTasksFinished[args.TaskNum] = true
	default:
		log.Fatalf("bad finished task %d", args.TaskType)

	}
	m.cond.Broadcast()
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
	ret := false
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	ret = m.isDone
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.cond = sync.NewCond(&m.mu)
	m.mapFiles = files
	m.nMapTasks = len(files)
	m.nReduceTasks = nReduce
	m.reduceTasksIssued = make([]time.Time, nReduce)
	m.reduceTasksFinished = make([]bool, nReduce)
	m.mapTasksIssued = make([]time.Time, len(files))
	m.mapTasksFinished = make([]bool, len(files))

	go func() {
		for {
			m.mu.Lock()
			m.cond.Broadcast()
			m.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
	m.server()
	return &m
}
