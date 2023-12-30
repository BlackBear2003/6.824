package mr

import "time"

type Task struct {
	Id       int
	TaskType int // 0:map, 1:reduce
	Filename string
	X        int
	Y        int
	NReduce  int
}

type TaskInfo struct {
	T         *Task
	DeadLine  time.Time
	Assigning bool
	Assigner  string
	Done      bool
}
