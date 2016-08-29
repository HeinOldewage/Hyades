package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
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

type ConfigFile struct {
	DataPath *string
	DB       *string
}

var configFilePath *string = flag.String("config", "config.json", "If the config file is specified it overrides command line paramters and defaults")

var configuration ConfigFile = ConfigFile{
	DataPath: flag.String("dataFolder", "userData", "The folder that the distribution server saves the data"),
	DB:       flag.String("DBFile", "../jobs.db", "Sqlite db file"),
}

func main() {
	fmt.Println("This is the distribution server")
	flag.Parse()

	if *configFilePath != "" {
		file, err := os.Open(*configFilePath)
		if err != nil {
			log.Println("Config open error", err)
			return
		}

		decoder := json.NewDecoder(file)
		err = decoder.Decode(&configuration)
		if err != nil {
			log.Println("Config parse error", err)
			return
		}
	}
	log.Println("config", configuration)

	db, err := NewDB(*configuration.DB)
	if err != nil {
		log.Println("DB create error", err)
		return
	}
	fmt.Println("Connected to DB")

	ws := NewWorkServer(":8080", db, *configuration.DataPath)

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

	Log      *log.Logger
	Stats    *workServerStats
	dataPath string
}

func NewWorkServer(address string, db *DB, dataPath string) *WorkServer {
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
		dataPath,
	}
	Log.Println("Work server successfully created")
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

func (ws *WorkServer) getWork() (*Hyades.Work, error) {
	return ws.db.GetNextJob()
}

func (ws *WorkServer) retryWork(work *Hyades.Work, err string) {
	work.Failed()
	work.SetStatus("In Queue after error " + err)
}

func (ws *WorkServer) doneWork(work *Hyades.Work, res *Hyades.WorkResult) error {
	err := work.Succeeded()
	if err != nil {
		return err
	}
	work.SetStatus("Saving work")
	err = ws.SaveResult(work, res)
	if err != nil {
		return err
	}

	work.SetStatus("Work done")

	//work.PartOf().Save(ws.db.session)
	return nil
}

func (ws *WorkServer) SaveResult(w *Hyades.Work, res *Hyades.WorkResult) error {
	//Get Job work was part of, Get person Job belonged to and then save under
	//Person.JobFolder\Job.JobID\Work.partID\

	//Save 3 parts
	//Env.zip -- iff len(Env) > 0
	//StdOut.txt
	//ErrOut.txtlogFile

	folder := filepath.Join(ws.dataPath, w.PartOf().JobFolder, w.PartOf().Name+fmt.Sprint(w.PartOf().Id), strconv.Itoa(w.Index()))
	err := os.MkdirAll(folder, os.ModeDir|os.ModePerm)
	if err != nil {
		ws.Log.Println(err)
		return err
	}
	if res.EnvLength > 0 {
		envfile, err := os.Create(filepath.Join(folder, "Env.zip"))
		if err != nil {
			ws.Log.Println(err)
		}
		defer envfile.Close()
		_, err = io.CopyN(envfile, res.GetEnv(), int64(res.EnvLength))
		if err != nil {
			ws.Log.Println(err)
			log.Println("SaveResult", err)
			return err
		}
	}

	stdout, err := os.Create(filepath.Join(folder, "StdOut.txt"))
	if err != nil {
		ws.Log.Println(err)
		return err
	}
	defer stdout.Close()
	stdout.Write(res.StdOutStream)

	errout, err := os.Create(filepath.Join(folder, "ErrOut.txt"))
	if err != nil {
		ws.Log.Println(err)
	}
	defer errout.Close()
	errout.Write(res.ErrOutStream)

	return nil
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
	//atomic.AddInt32(&wss.ConnectedClient[info.ComputerName+":"+info.OperatingSystem].PartsDone, 1)
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
