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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
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
	id := strconv.Itoa(os.Getpid())
	for {
		time.Sleep(20 * time.Millisecond)
		task := CallForAssignTask(id)
		if task == nil {
			continue
		}

		if task.TaskType == 0 {
			commits := execMapTask(task, mapf)
			CallForSubmitTask(id, commits, task)
		} else if task.TaskType == 1 {
			commits := execReduceTask(task, reducef)
			CallForSubmitTask(id, commits, task)
		}
	}
}

func CallForAssignTask(workerId string) *Task {

	// declare an argument structure.
	args := CallForAssignTaskArgs{
		WorkerId: workerId,
	}

	// declare a reply structure.
	reply := CallForAssignTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("apply task id: %v\n", reply.T.Id)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.T
}

func CallForSubmitTask(workerId string, commits []CommitFile, task *Task) {

	// declare an argument structure.
	args := CallForSubmitTaskArgs{
		WorkerId: workerId,
		Commits:  commits,
		T:        task,
	}

	// declare a reply structure.
	reply := CallForSubmitTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.SubmitTask", &args, &reply)
	if ok {
		//fmt.Printf("submit task id: %v\n", task.Id)
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
		os.Exit(-1)
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

func execMapTask(task *Task, mapf func(string, string) []KeyValue) []CommitFile {
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	intermediates := make([][]KeyValue, task.NReduce)
	commits := []CommitFile{}
	for _, kv := range kva {
		y := ihash(kv.Key) % task.NReduce
		intermediates[y] = append(intermediates[y], kv)
	}
	for i, intermediate := range intermediates {
		tempFile, err := os.CreateTemp(".", "mr-tmp-")
		if err != nil {
			log.Fatalln("Creating temp file failed!")
			break
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalln("Json encoding failed!")
				break
			}
		}
		realname := fmt.Sprintf("mr-%d-%d", task.X, i)
		tmpname := tempFile.Name()
		commit := CommitFile{
			RealFilename: realname,
			TmpFilename:  tmpname,
		}
		commits = append(commits, commit)
	}
	return commits
}

func execReduceTask(task *Task, reducef func(string, []string) string) []CommitFile {
	// processing intermediate
	i := 0
	filenamePattern := "mr-%d-%d"
	intermediate := []KeyValue{}
	for i < task.X {
		filename := fmt.Sprintf(filenamePattern, i, task.Y)
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		kva := []KeyValue{}
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		intermediate = append(intermediate, kva...)
		i++
	}
	sort.Sort(ByKey(intermediate))

	// out put from inter to file
	oname := fmt.Sprintf("mr-out-%d", task.Y)
	ofile, _ := os.CreateTemp(".", oname)

	i = 0
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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	commits := []CommitFile{}
	commit := CommitFile{
		RealFilename: oname,
		TmpFilename:  ofile.Name(),
	}
	commits = append(commits, commit)
	return commits
}
