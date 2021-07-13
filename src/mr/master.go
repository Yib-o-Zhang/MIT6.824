package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu                  sync.Mutex
	mapFiles            []string
	nMapTasks           int
	nReduceTasks        int
	mapTasksFinished    []bool
	mapTasksIssued      []time.Time
	reduceTasksFinished []bool
	reduceTasksIssued   []time.Time
	inGet               chan GetTaskArgs
	outGet              chan GetTaskReply
	inFin               chan FinishTaskArgs
	outFin              chan FinishTaskReply
	isDone              bool
	exist               chan bool
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
	m.inGet <- args
	thisReply := <-m.outGet
	reply.NMapTasks = thisReply.NMapTasks
	reply.NReduceTasks = thisReply.NReduceTasks
	reply.TaskNum = thisReply.TaskNum
	reply.TaskType = thisReply.TaskType
	reply.MapFile = thisReply.MapFile
	return nil
}

func (m *Master) HandleFinishedTask(args FinishTaskArgs, reply *FinishTaskReply) error {
	m.inFin <- args
	<-m.outFin
	reply = new(FinishTaskReply)
	return nil
}

func (m *Master) loop() {

	go func() {
		for {
			select {
			case <-m.inGet:
				AllMapDone := true
				reply := GetTaskReply{}
				reply.NMapTasks = m.nMapTasks
				reply.NReduceTasks = m.nReduceTasks
				replyHandled := false
				for i, thisMapDone := range m.mapTasksFinished {
					if !thisMapDone {
						AllMapDone = false
						if m.mapTasksIssued[i].IsZero() || m.mapTasksIssued[i].Second() > 10 {
							reply.TaskType = Map
							reply.TaskNum = i
							reply.MapFile = m.mapFiles[i]
							m.mapTasksIssued[i] = time.Now()
							m.outGet <- reply
							replyHandled = true
							break
						}
					}
				}
				log.Println("map replyhandled:", replyHandled)
				if !replyHandled && !AllMapDone {
					m.outGet <- GetTaskReply{}
					continue
				}
				if AllMapDone {
					AllReduceDone := true
					replyHandled := false

					for i, thisReduceDone := range m.reduceTasksFinished {
						if !thisReduceDone {
							AllReduceDone = false
							if m.reduceTasksIssued[i].IsZero() || m.reduceTasksIssued[i].Second() > 10 {
								reply.TaskType = Reduce
								reply.TaskNum = i
								m.reduceTasksIssued[i] = time.Now()
								m.outGet <- reply
								replyHandled = true
								break
							}
						}
					}
					if !replyHandled && !AllReduceDone {
						m.outGet <- GetTaskReply{}
						continue
					}

					if AllReduceDone {
						reply.TaskType = Done
						m.isDone = true
						m.outGet <- reply
					}
				}
			case finArg := <-m.inFin:
				reply := FinishTaskReply{}
				switch finArg.TaskType {
				case Map:
					m.mapTasksFinished[finArg.TaskNum] = true
					m.outFin <- reply
				case Reduce:
					m.reduceTasksFinished[finArg.TaskNum] = true
					m.outFin <- reply
				default:
					log.Fatalf("bad finished task %d", finArg.TaskType)

				}
			}
		}
	}()
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
	m.loop()
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
	m.mapFiles = files
	m.nMapTasks = len(files)
	m.nReduceTasks = nReduce
	m.reduceTasksIssued = make([]time.Time, nReduce)
	m.reduceTasksFinished = make([]bool, nReduce)
	m.mapTasksIssued = make([]time.Time, len(files))
	m.mapTasksFinished = make([]bool, len(files))
	m.exist = make(chan bool)
	m.inGet = make(chan GetTaskArgs)
	m.outGet = make(chan GetTaskReply)
	m.inFin = make(chan FinishTaskArgs)
	m.outFin = make(chan FinishTaskReply)

	m.server()
	return &m
}
