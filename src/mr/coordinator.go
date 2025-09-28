package mr

import (
	"fmt"
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
	mutex sync.Mutex
	mapTask []Task
	reduceTask []Task
	nReduce int
	nMap int
	taskTimeout time.Duration
}
type Task struct {
    id       int
    FileName string
    Type     string
    State    TaskState
	AssignedAt time.Time
}

type TaskState int
const (
    Unallocated TaskState = iota
    Running
    Completed
)
// Your code here -- RPC处理程序for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    allMapDone := true
    for i, t := range c.mapTask {
        if t.State == Unallocated {
            reply.X = i
            reply.FileName = t.FileName
            reply.NReduce = c.nReduce
            reply.Type = "map"
            reply.State = Run
            c.mapTask[i].State = Running
            fmt.Println("[map]", reply.FileName)

			c.startTimeoutWatcher("map", t.id)
			fmt.Printf("Assign map task %d -> %s\n", t.id, t.FileName)
            return nil
        }
        if t.State != Completed {
            allMapDone = false
        }
    }

    if !allMapDone {
        reply.Type = "wait"
        reply.State = Wait
        return nil
    }

    allReduceDone := true
    for j, t := range c.reduceTask {
        if t.State == Unallocated {
            reply.Y = j
            reply.Nmap = c.nMap
            reply.Type = "reduce"
            reply.State = Run
            c.reduceTask[j].State = Running
            fmt.Println("[reduce]", j)

			c.startTimeoutWatcher("reduce", t.id)
			fmt.Printf("Assign reduce task %d\n", t.id)
            return nil
        }
        if t.State != Completed {
            allReduceDone = false
        }
    }

    if !allReduceDone {
        reply.Type = "wait"
        reply.State = Wait
        return nil
    }

    reply.Type = "exit"
    reply.State = Exit
    return nil
}

// func (c *Coordinator) ReturnTask(args *TaskArgs, reply *TaskReply) error {
//     c.mutex.Lock()
//     defer c.mutex.Unlock()

//     if args.Y == -1 && args.X >= 0 && args.X < len(c.mapTask) {
//         c.mapTask[args.X].State = Completed
//     }

//     if args.X == -1 && args.Y >= 0 && args.Y < len(c.reduceTask) {
//         c.reduceTask[args.Y].State = Completed
//     }

//     return nil
// }
func (c *Coordinator) ReturnTask(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.X >= 0 && args.X < len(c.mapTask) {
		if c.mapTask[args.X].State != Completed {
			c.mapTask[args.X].State = Completed
		}
	}
	if args.Y >= 0 && args.Y < len(c.reduceTask) {
		if c.reduceTask[args.Y].State != Completed {
			c.reduceTask[args.Y].State = Completed
		}
	}
	return nil
}
//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _,l := range c.mapTask{
		if l.State != Completed{
			return false
		}
	}
	for _,l := range c.reduceTask{
		if(l.State != Completed){
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		taskTimeout: 10 * time.Second,
	}
	c.nReduce = nReduce
	for i,l := range files{
		task := Task{
			id : i,
			FileName: l,
			Type: "map",
			State: Unallocated,
		}
		fmt.Println(task)
		c.mapTask = append(c.mapTask, task)
		
	}
	c.nMap = len(files)
	for i := 0;i < nReduce;i++{
		task2 := Task{
			id : i,
			Type: "reduce",
			State: Unallocated,
		}
		c.reduceTask = append(c.reduceTask, task2)
	}

	c.server()
	return &c
}
func (c *Coordinator) startTimeoutWatcher(kind string, id int) {
	timeout := c.taskTimeout
	go func(){
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		<-timer.C
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if kind == "map"{
			if(id >= 0 && id < len(c.mapTask)){
				if c.mapTask[id].State == Running{
					if(time.Since(c.mapTask[id].AssignedAt) >= timeout){
						c.mapTask[id].State = Unallocated
					}
				}
			}
		}else if kind == "reduce"{
			if(id >= 0 && id < len(c.reduceTask)){
				if c.reduceTask[id].State == Running{
					if(time.Since(c.reduceTask[id].AssignedAt) >= timeout){
						c.reduceTask[id].State = Unallocated
					}
				}
			}
		}
	}()
}