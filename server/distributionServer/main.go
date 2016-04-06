package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HeinOldewage/Hyades"
)

var DBUsername *string = flag.String("DBUsername", "", "MongoDb username")
var DBPassword *string = flag.String("DBPassword", "", "MongoDb password")

func main() {
	flag.Parse()
	fmt.Println("This is the distribution server")
	db, err := NewDB(*DBUsername, *DBPassword)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("Connected to DB")
	//log.Println("Job:", db.GetNextJob())

	ws := NewWorkServer(":8080", db)

	ws.Listen()
}

type clientStats struct {
	Info        *Hyades.ClientInfo
	ConnectedAt time.Time
	PartsDone   int32
}

type workServerStats struct {
	sync.RWMutex
	Connects            int32
	Disconnects         int32
	PartsDone           int32
	JobsDone            int32
	ClientTimes         []time.Duration
	FrameWorkErrorCount int32
	JobErrorCount       int32
	ConnectedClient     map[string]*clientStats
}

type WorkServer struct {
	Address string

	db *DB

	Log   *log.Logger
	Stats *workServerStats
}

func NewWorkServer(address string, db *DB) *WorkServer {
	logFile, err := os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file", ":", err)
	}

	Log := log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile)

	res := &WorkServer{
		address,
		db,
		Log,
		&workServerStats{ClientTimes: make([]time.Duration, 0), ConnectedClient: make(map[string]*clientStats)},
	}
	Log.Println("Work server successfully created")
	log.Println("Do you see this?")
	return res
}

func (ws *WorkServer) Listen() {
	ws.Log.Println("About to listen on", ws.Address)
	conn, err := net.Listen("tcp", ws.Address)
	if err != nil {
		panic(err)
	}
	ws.Log.Println("Waiting on", ws.Address)
	for {
		cc, err := conn.Accept()
		if err != nil {
			panic(err)
		}
		go NewClient(ws).handle(cc, ws)
		ws.Log.Println("Got connection", cc.(*net.TCPConn).RemoteAddr())
	}
}

func (ws *WorkServer) StartJob(j *Hyades.Job) {

}

func (ws *WorkServer) StopJob(j *Hyades.Job) {

}

func (ws *WorkServer) GetWorkAvailable() int {

	return 0
}

func (ws *WorkServer) getWork() *Hyades.Work {
	return ws.db.GetNextJob()
}

func (ws *WorkServer) retryWork(work *Hyades.Work, err string) {
	work.Failed(ws.db.session)
	work.SetStatus("In Queue after error"+err, ws.db.session)
}

func (ws *WorkServer) doneWork(work *Hyades.Work, res *Hyades.WorkResult) {
	work.Succeeded(ws.db.session)
	work.SetStatus("Saving work", ws.db.session)
	ws.SaveResult(work, res)
	work.SetStatus("Work done", ws.db.session)
	atomic.AddInt32(&work.PartOf().NumPartsDone, 1)
	work.PartOf().Save(ws.db.session)
}

func (ws *WorkServer) SaveResult(w *Hyades.Work, res *Hyades.WorkResult) {
	//Get Job work was part of, Get person Job belonged to and then save under
	//Person.JobFolder\Job.JobID\Work.partID\

	//Save 3 parts
	//Env.zip -- iff len(Env) > 0
	//StdOut.txt
	//ErrOut.txtlogFile

	folder := filepath.Join("userData", w.PartOf().JobFolder, w.PartOf().Name, strconv.Itoa(w.Index()))
	os.MkdirAll(folder, os.ModeDir|os.ModePerm)
	if len(res.Env) > 0 {
		envfile, err := os.Create(filepath.Join(folder, "Env.zip"))
		if err != nil {
			ws.Log.Println(err)
		}
		defer envfile.Close()
		envfile.Write(res.Env)
	}

	stdout, err := os.Create(filepath.Join(folder, "StdOut.txt"))
	if err != nil {
		ws.Log.Println(err)
	}
	defer stdout.Close()
	stdout.Write(res.StdOutStream)

	errout, err := os.Create(filepath.Join(folder, "ErrOut.txt"))
	if err != nil {
		ws.Log.Println(err)
	}
	defer errout.Close()
	errout.Write(res.ErrOutStream)
}

func (wss *workServerStats) Connected() {
	atomic.AddInt32(&wss.Connects, 1)
}

func (wss *workServerStats) Disconnected(info *Hyades.ClientInfo) {
	atomic.AddInt32(&wss.Disconnects, 1)
	if info != nil {
		wss.Lock()
		defer wss.Unlock()
		if infoInMap, ok := wss.ConnectedClient[info.ComputerName+":"+info.OperatingSystem]; ok {
			delete(wss.ConnectedClient, info.ComputerName+":"+info.OperatingSystem)
			wss.ClientTimes = append(wss.ClientTimes, time.Since(infoInMap.ConnectedAt))
		}

	}

}

func (wss *workServerStats) FrameWorkError() {
	atomic.AddInt32(&wss.FrameWorkErrorCount, 1)
}

func (wss *workServerStats) JobError() {
	atomic.AddInt32(&wss.JobErrorCount, 1)
}

func (wss *workServerStats) DonePart(info *Hyades.ClientInfo) {
	atomic.AddInt32(&wss.PartsDone, 1)
	wss.RLock()
	defer wss.RUnlock()
	atomic.AddInt32(&wss.ConnectedClient[info.ComputerName+":"+info.OperatingSystem].PartsDone, 1)
}

func (wss *workServerStats) Announced(info *Hyades.ClientInfo) {
	wss.Lock()
	defer wss.Unlock()
	wss.ConnectedClient[info.ComputerName+":"+info.OperatingSystem] = &clientStats{
		info,
		time.Now(),
		0,
	}
}
