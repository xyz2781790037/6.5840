package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct {
	X int
	Y int
	Type string
}

type TaskReply struct {
	X int
	Y int
	FileName string
	State TypeState
	Type string
	NReduce int
	Nmap int
}
type TypeState int
const (
    Wait TypeState = iota
    Run
    Exit
)
// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
