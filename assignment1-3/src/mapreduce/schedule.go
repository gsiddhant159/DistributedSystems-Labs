package mapreduce

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	tasks := make(chan task, ntasks)
	out := make(chan task, ntasks)

	go populateTasks(tasks, ntasks, phase, mr.files)

	var nCompleted int
	for nCompleted < ntasks {
		select {
		case t := <-tasks:
			debug("%v: Assigning task %v\n", phase, t.number)
			go assignTask(t, out, mr, phase, nios)
		case t := <-out:
			debug("%v: received %v status from task %v\n", phase, t.status, t.number)
			if t.status {
				nCompleted += 1
				debug("%v: nCompleted = %v\n", phase, nCompleted)
			} else {
				tasks <- t
			}
		}
	}
	debug("Schedule: %v phase done\n", phase)
}

func populateTasks(tasks chan task, ntasks int, phase jobPhase, files []string) {
	for i := 0; i < ntasks; i++ {
		var nTask string
		switch phase {
		case mapPhase:
			nTask = files[i]
		case reducePhase:
			nTask = "reduce"
		}
		tasks <- task{number: i, filename: nTask}
		debug("%v: populated %v\n", i, len(tasks))
	}
	debug("Populated %v in tasks\n", len(tasks))
}

func assignTask(
	t task,
	out chan task,
	mr *Master,
	phase jobPhase,
	numOtherPhase int,
) {
	debug("%v: acquiring worker for task %v\n", phase, t.number)
	worker := <-mr.registerChannel
	doTaskArgs := DoTaskArgs{
		JobName:       mr.jobName,
		NumOtherPhase: numOtherPhase,
		Phase:         phase,
		File:          t.filename,
		TaskNumber:    t.number,
	}
	debug("%v: %v working with %v\n", phase, worker, doTaskArgs)
	t.status = call(worker, "Worker.DoTask", &doTaskArgs, nil)
	debug("%v: %v finished for %v\n", phase, worker, t.number)
	out <- t
	debug("%v: %v exited for %v\n", phase, worker, t.number)
	mr.registerChannel <- worker
	debug("%v: %v re-registered after %v\n", phase, worker, t.number)
}

type task struct {
	number   int
	filename string
	status   bool
}
