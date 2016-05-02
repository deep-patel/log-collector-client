package main

import "fmt"
import "github.com/hpcloud/tail"
import "flag"
import "strconv"


type Job struct {
    LogStr  string
}

func NewWorker(id int, workerPool chan chan Job) Worker {
    return Worker{
        id:         id,
        jobQueue:   make(chan Job),
        workerPool: workerPool,
        quitChan:   make(chan bool),
    }
}

type Worker struct {
    id         int
    jobQueue   chan Job
    workerPool chan chan Job
    quitChan   chan bool
}

type ServerDetails struct{
    host string
    port int
}

var serverDetails ServerDetails
var jobQueue chan Job
func (w Worker) start() {
    go func() {
        for {
            // Add my jobQueue to the worker pool.
            w.workerPool <- w.jobQueue

            select {
            case job := <-w.jobQueue:
                MakeCall("http://"+serverDetails.host+":"+strconv.Itoa(serverDetails.port)+"/work", job.LogStr)
            case <-w.quitChan:
                fmt.Printf("worker%d stopping\n", w.id)
                return
            }
        }
    }()
}

func (w Worker) stop() {
    go func() {
        w.quitChan <- true
    }()
}

// NewDispatcher creates, and returns a new Dispatcher object.
func NewDispatcher(jobQueue chan Job, maxWorkers int) *Dispatcher {
    workerPool := make(chan chan Job, maxWorkers)

    return &Dispatcher{
        jobQueue:   jobQueue,
        maxWorkers: maxWorkers,
        workerPool: workerPool,
    }
}

type Dispatcher struct {
    workerPool chan chan Job
    maxWorkers int
    jobQueue   chan Job
}

func (d *Dispatcher) run() {
    for i := 0; i < d.maxWorkers; i++ {
        worker := NewWorker(i+1, d.workerPool)
        worker.start()
    }

    go d.dispatch()
}

func (d *Dispatcher) dispatch() {
    for {
        select {
        case job := <-d.jobQueue:
            go func() {
                fmt.Printf("fetching workerJobQueue\n")
                workerJobQueue := <-d.workerPool
                fmt.Printf("adding\n",)
                workerJobQueue <- job
            }()
        }
    }
}

func add(logString string) {
    // Create Job and push the work onto the jobQueue.
    job := Job{LogStr: logString}
    jobQueue <- job
}



func main() {
    var (
        hostUrl   = flag.String("h", "localhost", "Server host")
        port = flag.Int("p", 9999, "Port of the server")
        followFile = flag.String("f","","Follow file")
        maxWorkers   = flag.Int("max_workers", 1, "The number of workers to start")
        maxQueueSize = flag.Int("max_queue_size", 100, "The size of job queue")
    )
    flag.Parse()

    error := 0
    if *followFile==""{
        fmt.Println("Provide file to be followed. Provide the same using -f <log file location>")
        error++
    }

    if error > 0{
        return;
    }

    serverDetails = ServerDetails{host: *hostUrl, port: *port}
    // Create the job queue.
    jobQueue = make(chan Job, *maxQueueSize)

    // Start the dispatcher.
    dispatcher := NewDispatcher(jobQueue, *maxWorkers)
    dispatcher.run()
	t, err := tail.TailFile("/Users/deep.patel/Documents/test.txt", tail.Config{
    Follow: true,
    ReOpen: true})
    if err == nil{
    	for line := range t.Lines {
            MakeCall("http://"+serverDetails.host+":"+strconv.Itoa(serverDetails.port)+"/work", line.Text)
             //add(line.Text)
		}
    } else{
    	fmt.Println("Error occured")
    	fmt.Println(err);
    }
}