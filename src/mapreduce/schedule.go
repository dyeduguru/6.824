package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	taskChannel := make(chan int, ntasks)
	go func() {
		for i:=0; i<ntasks; i++ {
			taskChannel <- i
		}
	}()
	var mutex = &sync.Mutex{}
	for {
		taskNum, more := <-taskChannel
		if !more {
			break
		}
		worker := <- registerChan
		go func() {
			args := new(DoTaskArgs)
			args.File = mapFiles[taskNum]
			args.JobName = jobName
			args.Phase = phase
			args.TaskNumber = taskNum
			args.NumOtherPhase = n_other
			ok := call(worker, "Worker.DoTask", args, new(struct{}))
			if ok == false {
				taskChannel <- taskNum
			} else {
				mutex.Lock()
				ntasks--
				if ntasks == 0 {
					close(taskChannel)
				}
				mutex.Unlock()
			}
			registerChan <- worker
		} ()
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
