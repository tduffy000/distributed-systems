package mapreduce
import (
  "container/list"
  "fmt"
)

type WorkerInfo struct {
  address string
}

// Clean up all workers by sending a Shutdown RPC to each one of them
// Collect the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {

  // buffered channels (for # of map & reduce jobs)
  mapsCompleted := make(chan bool, mr.nMap)
  reducesCompleted := make(chan bool, mr.nReduce)

  /*
   * MAP
   */
  for i := 0; i < mr.nMap; i++ {

    // goroutine for each map job (run in parallel)
    go func(id int) {

      for {
        // get a registered worker from channel
        wk := <- mr.registerChannel

        // avoid concurrency with mutual exclusion
        mr.mux.Lock()
        _, found := mr.Workers[wk]
        mr.mux.Unlock()
        if !found {
          mr.mux.Lock()
          mr.Workers[wk] = &WorkerInfo{wk}
          mr.mux.Unlock()
        }

        // packets to interact w/ remote (thru RPC)
        var reply DoJobReply;
        args := &DoJobArgs{}
        args.JobNumber = id
        args.File = mr.file
        args.Operation = Map
        args.NumOtherPhase = mr.nReduce

        // send RPC to worker
        ok := call(wk, "Worker.DoJob", args, &reply)

        // check reply from worker
        if !ok {
          fmt.Printf("Worker %s doMap error\n", wk)
        } else {
          mapsCompleted <- true  // did job => inform channel
          mr.registerChannel <- wk // send wk back to registered worker channel
          return
        }
      }
    }(i)
  }

  // move on to reduce when all map jobs have finished
  for i := 0; i < mr.nMap; i++ {
    <- mapsCompleted
  }
  fmt.Printf("Finished with all %d map jobs\n", mr.nMap)

  /*
   * REDUCE
   */
  for i := 0; i < mr.nReduce; i++ {

    // goroutine for each reduce job
    go func(id int) {
      for {

        wk := <- mr.registerChannel
        mr.mux.Lock()
        _, found := mr.Workers[wk]
        mr.mux.Unlock()
        if !found {
          mr.mux.Lock()
          mr.Workers[wk] = &WorkerInfo{wk}
          mr.mux.Unlock()
        }

        var reply DoJobReply;
        args := &DoJobArgs{}
        args.JobNumber = id
        args.File = mr.file
        args.Operation = Reduce
        args.NumOtherPhase = mr.nMap

        ok := call(wk, "Worker.DoJob", args, &reply)

        if !ok {
          fmt.Printf("Worker %s doReduce error\n", wk)
        } else {
          reducesCompleted <- true
          mr.registerChannel <- wk
          return
        }
      }
    }(i)
  }

  // move on when all reduce jobs have finished
  for i := 0; i < mr.nReduce; i++ {
    <- reducesCompleted
  }
  fmt.Printf("Finished with all %d reduce jobs", mr.nReduce)

  return mr.KillWorkers()
}
