package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	assigning chan *Task
	submiting chan *Task

	supervising chan *TaskInfo

	mapsign         chan int
	reducesign      chan int
	createReduceAck chan int
	donesign        chan int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *AssignArgs, reply *AssignReply) error {

	task := <-c.assigning
	reply.Task = task
	taskInfo := TaskInfo{}
	taskInfo.StartTime = time.Now()
	taskInfo.T = task

	c.supervising <- &taskInfo
	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitArgs, reply *SubmitReply) error {

	submit := args.Task

	c.submiting <- submit

	return nil
}

func (c *Coordinator) supervise() {
	isR := false
	for {
		if len(c.supervising)+len(c.assigning) == 0 {
			// fmt.Println("map over, time to reduce~")
			c.reducesign <- 1
			// fmt.Println("sended")
			ack := <-c.createReduceAck
			isR = true
			// fmt.Println("received")
			if ack != 1 {
				log.Fatalln("Something wrong when reduced tasks and ack back")
			}
			continue
		}
		if len(c.supervising) == 0 {
			// fmt.Println("nothing working")
			// sleep 300ms
			time.Sleep(300 * time.Millisecond)
			continue
		}
		// fmt.Printf("----------------------------- \n")
		// fmt.Printf("Supervising Task Left %d \n", len(c.supervising))
		// fmt.Printf("Assigning Task Left %d \n", len(c.assigning))
		// fmt.Printf("Submitting Left %d \n", len(c.submiting))

		isub := 0
		nsub := len(c.submiting)
		for isub < 2*nsub {
			if len(c.submiting) == 0 {
				break
			}
			submitted := <-c.submiting
			isup := 0
			nsup := len(c.supervising)
			for isup < 2*nsup {
				supervised := <-c.supervising

				if isR && submitted.ReduceTaskNum == supervised.T.ReduceTaskNum {
					// fmt.Printf("Task x-%d y-%d Submited\n", supervised.T.MapTaskNum, supervised.T.ReduceTaskNum)
					break
				} else if !isR && submitted.MapTaskNum == supervised.T.MapTaskNum {
					// fmt.Printf("Task x-%d y-%d Submited\n", supervised.T.MapTaskNum, supervised.T.ReduceTaskNum)
					break
				} else {

					duration := time.Since(supervised.StartTime)
					if duration > 10*time.Second {
						c.assigning <- supervised.T
						// fmt.Printf("Task x-%d y-%d Failed, added to Assigning channel\n", supervised.T.MapTaskNum, supervised.T.ReduceTaskNum)
					} else {
						// println("mei chao shi")
						c.supervising <- supervised
					}

				}
				isup++
			}

			isub++
		}
		isup := 0
		nsup := len(c.supervising)
		for isup < 2*nsup {
			supervised := <-c.supervising

			duration := time.Since(supervised.StartTime)
			if duration > 10*time.Second {
				c.assigning <- supervised.T
				// fmt.Printf("Task x-%d y-%d Failed, added to Assigning channel\n", supervised.T.MapTaskNum, supervised.T.ReduceTaskNum)
			} else {
				// println("mei chao shi")
				c.supervising <- supervised
			}

			isup++
		}
		time.Sleep(2000 * time.Millisecond)
	}

}

func (c *Coordinator) publishReduceTasks(x, y int) {

	sig := <-c.reducesign
	if sig != 1 {
		log.Fatal("Something wrong when ending map to reduce\n")
	}
	// fmt.Printf("Received Signal %d\n", sig)
	i := 0
	for i < y {
		task := Task{
			TaskType:      1,
			ReduceTaskNum: i,
			MapTaskNum:    x,
			NReduce:       y,
		}
		c.assigning <- &task
		i++
	}
	c.createReduceAck <- 1

	sig = <-c.reducesign
	if sig != 1 {
		log.Fatal("Something wrong when receiving signal\n")
	}
	c.donesign <- 1
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.

	// for first poc test
	// if len(c.tasks) == 0 {
	// 	ret = true
	// }

	return len(c.donesign) == 1
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// map
	c.assigning = make(chan *Task, 11)
	c.submiting = make(chan *Task, 11)
	c.supervising = make(chan *TaskInfo, 11)
	c.mapsign = make(chan int, 1)
	c.createReduceAck = make(chan int, 1)
	c.reducesign = make(chan int, 1)
	c.donesign = make(chan int, 1)

	// fmt.Println(len(files))

	i := 0
	for i < len(files) {
		task := Task{}
		task.TaskType = 0
		task.Files = files
		task.MapTaskNum = i
		task.ReduceTaskNum = 0
		task.NReduce = nReduce

		c.assigning <- &task
		i++
	}
	go c.supervise()
	go c.publishReduceTasks(len(files), nReduce)
	c.server()
	return &c
}
