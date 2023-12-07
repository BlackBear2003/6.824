package mr

import "time"

type Task struct {
	TaskType      int // 0:map , 1:reduce
	Files         []string
	MapTaskNum    int
	ReduceTaskNum int
	NReduce       int
}

type TaskInfo struct {
	T         *Task
	StartTime time.Time
}
