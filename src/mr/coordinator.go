package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	// Your definitions here.
	mutex sync.Mutex
	mapTask []Task
	reduceTask []Task
	nReduce int

}
type Task struct {
    id       int
    FileName string
    Type     string
    State    TaskState
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
func (c *Coordinator) AssaginTask(args *ExampleArgs, reply *ExampleReply) error {
	
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
	c := Coordinator{}
	c.nReduce = nReduce
	for i,l := range files{
		task := Task{
			id : i,
			FileName: l,
			Type: "map",
			State: Unallocated,
		}
		c.mapTask = append(c.mapTask, task)
	}
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