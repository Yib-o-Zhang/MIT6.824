package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		call("Master.HandleGetTask", args, &reply)
		log.Println("TaskType:", reply.TaskType)
		switch reply.TaskType {
		case Map:
			doMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf)
		case Reduce:
			doReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			os.Exit(0)
		case Wait:
			time.Sleep(1 * time.Second)
			continue
		default:
			panic("bad task type")
		}

		finArgs := FinishTaskArgs{
			reply.TaskType,
			reply.TaskNum,
		}
		finReply := FinishTaskReply{}
		call("Master.HandleFinishedTask", finArgs, &finReply)

	}

}

func doReduce(ThisTaskN int, NMapTask int, reducef func(string, []string) string) {
	//assume that we can sort data in memory
	var kva []KeyValue
	for m := 0; m < NMapTask; m++ {
		iFileName := getIntermediateFile(m, ThisTaskN)
		file, err := os.Open(iFileName)
		if err != nil {
			log.Fatalf("can't open intermediateFile in redue task")
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	log.Println("kv sorted")
	tempFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("can not creare tmp file ")
	}
	tempFileName := tempFile.Name()

	i := 0
	for i < len(kva) {
		j := i + 1
		var values []string
		values = append(values, kva[i].Value)
		for j < len(kva) && (kva[i].Key == kva[j].Key) {
			values = append(values, kva[j].Value)
			j++
		}
		reduceOut := reducef(kva[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, reduceOut)

		i = j
	}
	finalizeReduceFile(tempFileName, ThisTaskN)

}

func doMap(filename string, thisTaskN int, ReduceTasksN int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("can not open file : %v", filename)
	}
	defer file.Close()
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("can not read file : %v", filename)
	}
	kva := mapf(filename, string(contents))
	var tmpfiles []*os.File
	var tmpfileName []string
	var encoders []*json.Encoder
	for r := 0; r < ReduceTasksN; r++ {
		tempFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("can't open tmpfile")
		}
		tmpfiles = append(tmpfiles, tempFile)
		tmpfileName = append(tmpfileName, tempFile.Name())
		enc := json.NewEncoder(tempFile)
		encoders = append(encoders, enc)
	}
	for _, kv := range kva {
		r := ihash(kv.Key) % ReduceTasksN
		err := encoders[r].Encode(&kv)
		if err != nil {
			log.Fatalf("can't encode tmpfile")
		}
	}
	for _, f := range tmpfiles {
		f.Close()
	}
	for n := range tmpfiles {
		finalizeIntermediateFile(tmpfileName[n], thisTaskN, n)
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

// help functions to handle files

func finalizeReduceFile(tmpFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	os.Rename(tmpFile, finalFile)
}

func getIntermediateFile(mapTaskN int, reduceTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, reduceTaskN)
}

func finalizeIntermediateFile(tmpFile string, mapTaskN int, reduceTaskN int) {
	finalIntermediateFile := getIntermediateFile(mapTaskN, reduceTaskN)
	os.Rename(tmpFile, finalIntermediateFile)
}
