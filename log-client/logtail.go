package main

import "fmt"
import "github.com/hpcloud/tail"
import "flag"
import "strconv"
import "github.com/deep-patel/log-collector-client/utils"
import "errors"
import "log"

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

type ClientConfig struct{
    followFile string
    maxWorkers int
    maxQueueSize int
    multiThread bool
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
var clientConfig ClientConfig

func (w Worker) start() {
    go func() {
        for {
            // Add my jobQueue to the worker pool.
            w.workerPool <- w.jobQueue

            select {
            case job := <-w.jobQueue:
                utils.MakeCall("http://"+serverDetails.host+":"+strconv.Itoa(serverDetails.port)+"/work", job.LogStr)
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
        configFile = flag.String("c","","Follow file")
    )
    flag.Parse()

    error := 0
    if *configFile==""{
        fmt.Println("Provide configuration file. Provide the same using -c <log file location>")
        error++
    }

    if error > 0{
        return;
    }

    err := validateConfigFile(*configFile)
    if err != nil{
        fmt.Println(err)
        return
    }

    serverDetails = ServerDetails{host: *hostUrl, port: *port}
    // Create the job queue.
    jobQueue = make(chan Job, clientConfig.maxQueueSize)

    // Start the dispatcher.
    dispatcher := NewDispatcher(jobQueue, clientConfig.maxWorkers)
    dispatcher.run()
	t, err := tail.TailFile(clientConfig.followFile, tail.Config{
                Follow: true,
                ReOpen: true,
                Poll : true,
            })
    
    if err != nil {
        log.Fatalln(err)
    }
    if err == nil{
        fmt.Println(t.Lines)
    	for line := range t.Lines {
            if clientConfig.multiThread{
                add(line.Text)
            } else{
                utils.MakeCall("http://"+serverDetails.host+":"+strconv.Itoa(serverDetails.port)+"/work", line.Text)
            }
		}
    } else{
    	fmt.Println("Error occured")
    	fmt.Println(err);
    }
}


func validateConfigFile(configFileLocation string) error{
    config := make(map[string]string)
    err := utils.Load(configFileLocation, config)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%v\n", config)

    followFileTemp := config["followfile"]
    maxWorkerTemp, err1 := strconv.Atoi(config["maxworkers"])
    maxQueueSizeTemp, err2 := strconv.Atoi(config["maxqueuesize"])
    multiThreadTemp, err3 := strconv.ParseBool(config["multithread"])
    
    if followFileTemp == ""{
        return errors.New("followFile cannot be empty or nil")
    }
    
    if err1 != nil || maxWorkerTemp <= 0{
        fmt.Printf("maxWorkers found nil or less than equal to 0. maxWorkers: %d. Inititalizing it to default 5", maxWorkerTemp)
        maxWorkerTemp = 5
    }
    

    if err2 != nil || maxQueueSizeTemp <= 0{
        fmt.Printf("maxQueueSize found nil or less than equal to 0. maxQueueSize: %d. Inititalizing it to default 100", maxQueueSizeTemp)
        maxQueueSizeTemp = 100
    }

    if err3 != nil {
        fmt.Printf("multiThread found nil. Inititalizing it to default false")
        multiThreadTemp = false
    }
    

    clientConfig = ClientConfig{followFile: followFileTemp, maxWorkers: maxWorkerTemp, maxQueueSize: maxQueueSizeTemp, multiThread: multiThreadTemp}

    return nil

}