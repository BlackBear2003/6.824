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

type Coordinator struct {
	// Your definitions here.
	lock      sync.Mutex
	taskInfos map[int]*TaskInfo
	files     []string
	nReduce   int
	assigning chan *Task
	stage     int // 0: mapping, 1: reducing, 2: done
	tasknum   int
}

// Your code here -- RPC handlers for the worker to call.
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
	ret := false
	c.lock.Lock()
	if c.stage == 2 {
		ret = true
	}
	c.lock.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:     files,
		nReduce:   nReduce,
		stage:     0,
		assigning: make(chan *Task, 10),
	}
	c.InitMapTasks()

	c.server()

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			c.supervise()
			if c.stage == 2 {
				c.lock.Unlock()
				break
			}
			c.lock.Unlock()
		}
	}()

	return &c
}

func (c *Coordinator) InitMapTasks() {
	i := 0
	tasknum := len(c.files)
	c.tasknum = tasknum
	//c.assigning = make(chan *Task, tasknum)
	c.taskInfos = make(map[int]*TaskInfo)
	for i < tasknum {
		task := Task{
			Id:       i,
			TaskType: 0,
			Filename: c.files[i],
			X:        i,
			Y:        -1,
			NReduce:  c.nReduce,
		}
		meta := TaskInfo{
			T:         &task,
			Done:      false,
			Assigning: false,
			Assigner:  "",
		}

		c.assigning <- &task
		c.taskInfos[task.Id] = &meta
		i++
	}
}

func (c *Coordinator) InitReduceTasks() {
	i := 0
	tasknum := c.nReduce
	c.tasknum = tasknum
	//c.assigning = make(chan *Task, tasknum)
	c.taskInfos = make(map[int]*TaskInfo)
	for i < tasknum {
		task := Task{
			Id:       i,
			TaskType: 1,
			X:        len(c.files),
			Y:        i,
			NReduce:  c.nReduce,
		}
		meta := TaskInfo{
			T:         &task,
			Done:      false,
			Assigning: false,
			Assigner:  "",
		}

		c.assigning <- &task
		c.taskInfos[task.Id] = &meta
		i++
	}
}

// RPC side
func (c *Coordinator) AssignTask(args *CallForAssignTaskArgs, reply *CallForAssignTaskReply) error {
	task, ok := <-c.assigning
	if !ok { // Channel closed, Map and Reduce both done
		return nil
	}
	c.lock.Lock()

	meta := c.taskInfos[task.Id]

	meta.Assigner = args.WorkerId
	meta.Assigning = true
	meta.DeadLine = time.Now().Add(time.Second * 10)
	//fmt.Printf("assign task %d to worker %s \n", meta.T.Id, meta.Assigner)

	reply.T = task
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) SubmitTask(args *CallForSubmitTaskArgs, reply *CallForSubmitTaskReply) error {

	c.lock.Lock()
	task := args.T
	meta := c.taskInfos[task.Id]
	//fmt.Printf("- submit task:%d from: %s meta: %s \n", meta.T.Id, args.WorkerId, meta.Assigner)
	// judge if the task is still assigned to worker
	if args.WorkerId == meta.Assigner {
		//fmt.Println("yes")
		// believe first
		meta.Done = true
		// commit filename
		for _, commit := range args.Commits {
			err := os.Rename(commit.TmpFilename, commit.RealFilename)
			if err != nil {
				log.Printf("Rename tmpfile failed for %s\n", commit.RealFilename)
				// deny after
				meta.Done = false
			}
		}
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) supervise() {
	n := 0
	for _, meta := range c.taskInfos {
		if meta.Done {
			n++
			continue
		}
		// judge if out of time
		if meta.Assigning && !meta.Done && meta.Assigner != "" {
			if time.Now().After(meta.DeadLine) {
				//fmt.Printf("task: %d out of time\n", meta.T.Id)
				meta.Assigning = false
				meta.Assigner = ""
				c.assigning <- meta.T
			}
		}
	}
	// all done
	if n == c.tasknum {
		c.stage++
		if c.stage == 1 {
			//fmt.Println("To Reduce Stage")
			c.InitReduceTasks()
		}
		if c.stage == 2 {
			close(c.assigning)
		}
	}
}
