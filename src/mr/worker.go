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

	// Your worker implementation here.
	for {
		t := CallForAssignWork()
		if t.TaskType == 0 {
			filenames := t.Files
			filename := filenames[t.MapTaskNum]
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
			intermediates := make([][]KeyValue, t.NReduce)

			for _, kv := range kva {
				y := ihash(kv.Key) % t.NReduce
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
				realname := fmt.Sprintf("mr-%d-%d", t.MapTaskNum, i)
				err = os.Rename(tempFile.Name(), realname)
				if err != nil {
					log.Printf("Rename tmpfile failed for %s\n", realname)
				}
			}
			CallForSubmitWork(t)
		} else if t.TaskType == 1 {
			// processing intermediate
			i := 0
			filenamePattern := "mr-%d-%d"
			intermediate := []KeyValue{}
			for i < t.MapTaskNum {
				filename := fmt.Sprintf(filenamePattern, i, t.ReduceTaskNum)
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
			oname := fmt.Sprintf("mr-out-%d", t.ReduceTaskNum)
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
			err := os.Rename(ofile.Name(), oname)
			if err != nil {
				log.Printf("Rename tmpfile failed for %s\n", oname)
			}
			CallForSubmitWork(t)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallForAssignWork() *Task {
	args := AssignArgs{}

	reply := AssignReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// fmt.Printf("reply.task x=%v y=%v\n", reply.Task.MapTaskNum, reply.Task.ReduceTaskNum)
		return reply.Task
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

func CallForSubmitWork(task *Task) {
	args := SubmitArgs{}
	args.Task = task

	reply := SubmitReply{}

	ok := call("Coordinator.SubmitTask", &args, &reply)
	if ok {
		// fmt.Printf("submit.task x=%v y=%v\n", args.Task.MapTaskNum, args.Task.ReduceTaskNum)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
	os.Exit(-1)
	fmt.Println(err)

	return false
}
