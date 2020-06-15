package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"proj1/feed"
	"strconv"
	"sync"
	"sync/atomic"
)

func main() {
	if len(os.Args) != 3{
		sequentialTwitter()
	} else {
		parallelTwitter()
	}
}

func sequentialTwitter(){
	//fmt.Println("**IN SEQUENTIAL MODE**")
	tf := feed.NewFeed()
	tq := NewTaskQueue()
	boolTasksLeftToEnqueue := new(bool)
	*boolTasksLeftToEnqueue = true
	doneOccurred := new(bool)
	*doneOccurred = false
	totalTasksLeft := new(int64)

	producer(tq, boolTasksLeftToEnqueue, totalTasksLeft)

	for {
		dequeuedTask := tq.Dequeue(1)[0]
		if dequeuedTask.Command == "DONE"{
			return
		}
		executeTask(tq, dequeuedTask, tf, doneOccurred)
	}
	return
}

func parallelTwitter(){
	numRoutines, _ := strconv.Atoi(os.Args[1])// is an integer and represents the number of goroutines to spawn (ie number of consumers to spawn)
	blockSize, _ := strconv.Atoi(os.Args[2]) // is an integer and represents the maximum amount of tasks a goroutine should be processing at any time
	tq := NewTaskQueue()
	tf := feed.NewFeed()
	var group sync.WaitGroup
	boolTasksLeftToEnqueue := new(bool)
	*boolTasksLeftToEnqueue = true
	doneOccurred := new(bool)
	*doneOccurred = false
	totalTasksLeft := new(int64)

	//spawn consumer goroutines
	for i := 0; i < numRoutines; i++{
		group.Add(1)
		go consumer(tq, tf, blockSize, &group, totalTasksLeft, doneOccurred)
	}

	producer(tq, boolTasksLeftToEnqueue, totalTasksLeft)
	group.Wait()
	//fmt.Println("finished go routine-------------------------")
	return
}

//each consumer will try to grab at least a blockSize amount of tasks from the queue. If there is less than blockSize amount, then the
//consumer grabs all tasks from the queue and executes them. When a consumer finishes executing its block of tasks, it checks the queue
//to grab another blockSize amount of tasks. If there are no tasks in the queue it will need to wait for more tasks to process or exit if there
//are no remaining tasks
func consumer(tq TaskQueue, tf feed.Feed, blockSize int,  group *sync.WaitGroup, totalTasksLeft *int64, doneOccurred *bool){
	var dequeuedTasks [] *Task
	for *totalTasksLeft > 0 || *doneOccurred == false{
	//for true {

		if int(*totalTasksLeft) < blockSize{
			dequeuedTasks = tq.Dequeue(int(*totalTasksLeft))
			atomic.AddInt64(totalTasksLeft, -int64(len(dequeuedTasks)))
			for i := 0; i < len(dequeuedTasks); i++ {
				executeTask(tq, dequeuedTasks[i], tf, doneOccurred)
			}
		} else {
			dequeuedTasks = tq.Dequeue(blockSize)
			atomic.AddInt64(totalTasksLeft, -int64(blockSize))
			for i := 0; i < len(dequeuedTasks); i++ {
				executeTask(tq, dequeuedTasks[i], tf, doneOccurred)
			}
		}
		if *doneOccurred == true{
			//*totalTasksLeft = 0
			break
		}
		fmt.Println("testing", *doneOccurred, *totalTasksLeft)
	}
	group.Done()
	fmt.Println("finished consumer -----------------")
	return
}

//producer function's job is to read in from os.Stdin a series of tasks and enqueue them into our task queue
//if there is at least one consumer goroutine waiting for work then place a task inside the queue and wake the one consumer up
//otherwise, the main goroutine continues to place tasks into the queue. Eventually, the consumers will grab the tasks from the queue at later point in time
func producer(tq TaskQueue, boolTasksLeftToEnqueue *bool, totalTasksLeft *int64){
	dec := json.NewDecoder(os.Stdin)
	for { //loop through and process each json object as task
		var t Task
		err := dec.Decode(&t)
		if err != nil {
			fmt.Println(err)
		} else {
			*totalTasksLeft++
		}
		tq.Enqueue(t.Command, t.Body, t.Id, t.Timestamp)
		if t.Command == "DONE"{
			break //guaranteed that DONE will be the last command of our stdin tasks
		}
	}
	*boolTasksLeftToEnqueue = false
}

//once a task is dequeued off of our task queue, execute its command onto our feed
func executeTask(tq TaskQueue, task *Task, tf feed.Feed, doneOccurred *bool){
	if task.Command == "ADD" {
		tf.Add(task.Body, task.Timestamp)
		printResponse(true, task.Id)
	} else if task.Command == "REMOVE" {
		success := tf.Remove(task.Timestamp)
		printResponse(success, task.Id)
	} else if task.Command == "CONTAINS" {
		success := tf.Contains(task.Timestamp)
		printResponse(success, task.Id)
	} else if task.Command == "FEED"{
		listFeedResponse := tf.Feed()
		printResponseFeed(task.Id, listFeedResponse)
	} else if task.Command == "DONE"{
		fmt.Println("EXECUTED DONE TASK")
		*doneOccurred = true
		tq.DoneBroadcast()
	} else {
		fmt.Println("Command ", task.Command, "not recognized")
	}
}

// prints response in string json format to stdout
func printResponse(success bool, id int){
	response := Response{success, id}
	responseMarshal, err := json.MarshalIndent(response, "", "     ")
	if err != nil {
		log.Fatal("Failure in json printing", err)
	}
	fmt.Println(string(responseMarshal))
}

func printResponseFeed(id int, listFeedResponse []feed.StructFeedResponse) {
	response := ResponseFeed{id, listFeedResponse}
	responseMarshal, err := json.MarshalIndent(response, "", "     ")
	if err != nil {
		log.Fatal("Failure in json printing", err)
	}
	fmt.Println(string(responseMarshal))
}

// Internal representation of tasks
type Task struct {
	Command string `json:"command"` //represents the type of feed task
	Body string `json:"body"`// the text of the task
	Id int `json:"id"`//unique identifier of task
	Timestamp float64  `json:"timestamp"`// Unix timestamp of the task
	next *Task  // the next task in the feed
}

// Internal representation of responses
type Response struct {
	Success bool `json:"success"`
	Id int `json:"id"`
}

// Internal representation of response feed
type ResponseFeed struct {
	Id int `json:"id"`
	Feed []feed.StructFeedResponse `json:"feed"`
}

// A twitter task queue
type TaskQueue interface {
	Enqueue(command string, body string, id int,  timestamp float64)
	Dequeue(blockSize int) []*Task
	GetSize() *uint64
	DoneBroadcast()
}

//feed is the internal representation of a user's task queue
type taskQueue struct {
	enqLock sync.Mutex // not needed since one producer
	deqLock sync.Mutex
	lock sync.Mutex
	notEmpty *sync.Cond
	head *Task
	tail *Task
	size *uint64
	doneOccurred *bool
}

//creates new task queue
func NewTaskQueue() TaskQueue {
	q := taskQueue{}
	q.enqLock = sync.Mutex{}
	q.deqLock = sync.Mutex{}
	q.lock = sync.Mutex{}
	q.notEmpty = sync.NewCond(&q.lock)
	q.head = newTaskEmpty() //placeholder sentinel node
	q.tail = q.head //placeholder sentinel node
	q.size = new(uint64)
	q.doneOccurred = new(bool)
	*q.doneOccurred = false
	return &q
}

//NewPost creates and returns a new post value given its body and timestamp
func newTask(command string, body string, id int,  timestamp float64, next *Task) *Task {
	return &Task{command,body, id,  timestamp, next}
}

func newTaskEmpty() *Task{
	return &Task{"sentinel", "sentinel", 0, 0, nil}
}

//****************************************
//****************************************
//**Using coarse grained synchronization**
//****************************************
//****************************************

//Dequeues new task from beginning of our queue (FIFO)
func (q *taskQueue) Dequeue(blockSize int) []*Task{
	var removedTasks [] *Task
	var removedTask *Task
	q.lock.Lock()

	for i:=0; i < blockSize; i++{
		//wait if queue is empty and if done has not occurred
		for *q.size == 0  && *q.doneOccurred == false {
			q.notEmpty.Wait()
		}
		if *q.doneOccurred {
			q.lock.Unlock()
			return removedTasks
		}
		removedTask = q.head.next
		removedTasks = append(removedTasks, removedTask)
		q.head = q.head.next
		atomic.CompareAndSwapUint64(q.size, *q.size, *q.size -1)

		if removedTask.Command == "DONE"{
			fmt.Println("DEQUEUED DONE TASK")
			q.lock.Unlock()
			return removedTasks
		}
	}
	q.lock.Unlock()
	return removedTasks
}

//Enqueues new task on to the end of our queue
func (q *taskQueue) Enqueue(command string, body string, id int,  timestamp float64){
	q.lock.Lock()
	newNode := newTask(command, body, id, timestamp, nil)
	q.tail.next = newNode
	q.tail = newNode
	atomic.AddUint64(q.size, 1)
	if *q.size > 0 {
		q.notEmpty.Broadcast()
	}
	q.lock.Unlock()
}

func (q *taskQueue) GetSize() *uint64{
	return q.size
}

func (q *taskQueue) DoneBroadcast(){
	*q.doneOccurred = true
	q.notEmpty.Broadcast()
}


