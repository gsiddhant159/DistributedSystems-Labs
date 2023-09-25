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
	completed := make(chan task, ntasks)

	go populateTasks(tasks, ntasks, phase, mr.files)

	for len(completed) < ntasks {
		if len(tasks) == 0 {
			continue // This short cictuits the loop when no more tasks to schedule
		}
		debug("%v: Completed: %v tasks\n", phase, len(completed))
		worker := <-mr.registerChannel
		debug("%v: Assigning %v with %v tasks\n", phase, worker, len(tasks))
		go assignTask(
			tasks,
			completed,
			mr.registerChannel,
			worker,
			mr.jobName,
			phase,
			nios,
		)
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
	tasks chan task,
	completed chan task,
	registerChannel chan string,
	worker, jobName string,
	phase jobPhase,
	numOtherPhase int,
) {
	debug("%v: AssignTask: on %v with %v tasks\n", phase, worker, len(tasks))
	t := <-tasks
	doTaskArgs := DoTaskArgs{
		JobName:       jobName,
		NumOtherPhase: numOtherPhase,
		Phase:         phase,
		File:          t.filename,
		TaskNumber:    t.number,
	}
	debug("%v: %v working with %v\n", phase, worker, doTaskArgs)
	ok := call(worker, "Worker.DoTask", &doTaskArgs, nil)
	if ok {
		completed <- t
	} else {
		debug("Error occured in rpc call to Worker %v\n", worker)
		tasks <- t
	}
	registerChannel <- worker
}

type task struct {
	number   int
	filename string
}
