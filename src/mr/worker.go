package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
        args := TaskArgs{}
        reply := TaskReply{}
		CallTask(&args,&reply)

		switch reply.Type{
			case "map":
				handleMapTask(&reply,mapf)
				CallReturn(&args,&reply)
			case "reduce":
				handleReduceTask(&reply,reducef)
				CallReturn(&args,&reply)
			case "wait":
				time.Sleep(time.Second)
			case "exit":
				return
		}
	}
}
func check(e error) {
    if e != nil {
        panic(e)
    }
}
func handleMapTask(reply *TaskReply,mapf func(string, string) []KeyValue) {
	data,err := os.ReadFile(reply.FileName)
	files := make([]*os.File,reply.NReduce)
	check(err)
	contents := string(data)
	fmt.Println(reply.FileName, len(contents))
	hmap := mapf(reply.FileName,contents)
	encoders := make([]*json.Encoder, reply.NReduce)
	for i := 0;i < reply.NReduce;i++{
		oname := fmt.Sprintf("mr-%d-%d", reply.X, i)
		file,_ := os.Create(oname)
		files[i] = file
        encoders[i] = json.NewEncoder(file)//创建一个json编码器，并放入 encoders[i]
	}
	for _,l := range hmap{
		num := ihash(l.Key) % reply.NReduce
		err := encoders[num].Encode(&l)//写入
        check(err)
	}
	for i := 0;i < reply.NReduce;i++{
		files[i].Close()
	}
}
func handleReduceTask(reply *TaskReply,reducef func(string, []string) string){
	interm := make(map[string][]string)
	for i := 0;i < reply.Nmap;i++{
		fileName := fmt.Sprintf("mr-%d-%d",i,reply.Y)
		file,err := os.Open(fileName)
		if err != nil{
			continue
		}
		dec := json.NewDecoder(file)
		for{
			var kv KeyValue
        	if err := dec.Decode(&kv); err != nil {
            	break
        	}
        	interm[kv.Key] = append(interm[kv.Key], kv.Value)
		}
		file.Close()
	}
	var keys []string
	for k := range interm{
		keys = append(keys, k)
	}
	sort.Strings(keys)

	finalName := fmt.Sprintf("mr-out-%d",reply.Y)
	ofile,_ := os.Create(finalName)
	defer ofile.Close()
	for _,k := range keys{
		output := reducef(k, interm[k])
		// fmt.Println("{",output,"}")
		fmt.Fprintf(ofile,"%v %v\n",k,output)
	}
	
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallTask(args *TaskArgs, reply *TaskReply) {

	// declare an argument structure.
	// fill in the argument(s).

	// declare a reply structure.

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// fmt.Printf("I reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
func CallReturn(args *TaskArgs, reply *TaskReply) {
	switch reply.Type{
	case "map":
		args.X = reply.X
		args.Y = -1
	case "reduce":
		args.Y = reply.Y
		args.X = -1
	default:
		return
	}
	tmp := TaskReply{}
	ok := call("Coordinator.ReturnTask", &args, &tmp)
	if ok {
		// fmt.Printf("\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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

	fmt.Println(err)
	return false
}
