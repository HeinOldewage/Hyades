package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"

	"github.com/HeinOldewage/Hyades"
	//"sync/atomic"
	_ "net/http/pprof"
	"time"
)

const (
	start  int = 0
	stop   int = 1
	pause  int = 2
	resume int = 3
)

var ServerAddress0 *string = flag.String("ServerAddress", "0.0.0.0:55555", "The server to get Jobs from")
var ServerAddress1 *string = flag.String("ServerAddress1", "1.1.1.1:55555", "The backup server to get Jobs from")
var logFile *string = flag.String("log", "", "The File to log to, if blank logging is done to stdout")
var retryTime *int = flag.Int("retry", 120, "The client will attempt to connect to the server approximately every so many seconds")
var jiggleTime *int = flag.Int("jiggle", 5, "The upper bound for the random interval by which retry time is offset")
var heartbeat *int = flag.Int("heartbeat", 120, "Time interval (in minutes) that the heartbeat will be sent")

func main() {
	go http.ListenAndServe(":8081", nil)
	flag.Parse()
	if *logFile != "" {
		file, err := os.Create(*logFile)
		if err == nil {
			defer file.Close()
			log.SetOutput(file)
		} else {
			log.Println("When creating log file", err)
		}
	}
	log.Println("A Client that executes jobs")

	go func() {
		for {
			StartComms(*ServerAddress0, *ServerAddress1)
			dura := *retryTime + rand.Intn(*jiggleTime)
			time.Sleep(time.Duration(dura) * time.Second)
		}
	}()

	events := HandleService()
	if events != nil {

		for e := range events {

			switch e {
			case start:
			case stop:
				log.Println("Service is stopped")
				//The loop will stop once events is closed. This allows the acknowledgement to be sent.
			case pause:
			case resume:
			}
		}
	} else {
		select {}
	}

}

func DoWork(work *Hyades.WorkComms, resChan chan *Hyades.WorkResult) {
	res := &Hyades.WorkResult{make([]byte, 0), make([]byte, 0), make([]byte, 0), "", 0}
	TempJobFolder := filepath.Join("Env", "Temp")
	log.Println("Making folder:[", TempJobFolder, "]")

	err := os.MkdirAll(TempJobFolder, os.ModeDir|os.ModePerm)
	if err != nil {
		log.Println("Error creating folder:", err)
		res.Error = err.Error()
		resChan <- res
		return
	}
	//defer os.RemoveAll(TempJobFolder)

	envreader := bytes.NewReader(work.Env)
	unzipper, err := zip.NewReader(envreader, int64(len(work.Env)))

	if err != nil {
		log.Println("Error zip.NewReader:", err)
		res.Error = err.Error()
		resChan <- res
		return
	}

	for _, file := range unzipper.File {
		os.MkdirAll(filepath.Join(TempJobFolder, path.Dir(file.Name)), os.ModeDir|os.ModePerm)

		outfile, _ := os.Create(filepath.Join(TempJobFolder, file.Name))
		zf, err := file.Open()
		if err != nil {
			log.Println("Error reading zip:", err)
			res.Error = err.Error()
			resChan <- res
			return
		}
		io.Copy(outfile, zf)
		outfile.Close()
		zf.Close()
	}

	stdBuf := new(bytes.Buffer)
	errBuf := new(bytes.Buffer)
	var cmd *exec.Cmd
	if runtime.GOOS == "linux" {
		log.Println("Setting up the linux ccommand", work)
		cmd = exec.Command(work.Parts.Command, work.Parts.Parameters)
	} else {
		log.Println("Setting up the windows ccommand")
		cmd = exec.Command("cmd", "/C", "cd "+TempJobFolder, " & "+work.Parts.Command+" "+work.Parts.Parameters)
	}

	fullpath, _ := filepath.Abs(TempJobFolder)
	fullpath = "\"" + fullpath + "\""

	log.Println("Running command", cmd)
	cmd.Dir = TempJobFolder
	cmd.Stdout = stdBuf
	cmd.Stderr = errBuf
	err = cmd.Run()
	if err != nil {
		log.Println("Error running command:", err)
		res.Error = err.Error()
		resChan <- res
		return
	}
	cmd.StdoutPipe()
	log.Println("Done command", cmd)

	log.Println("Std", string(stdBuf.Bytes()), len(stdBuf.Bytes()))
	log.Println("Err", string(errBuf.Bytes()), len(errBuf.Bytes()))

	//Delete any exes in the folder; they don't need to be sent back to the server

	scan, err := os.Open(TempJobFolder)
	if err != nil {
		panic(err)
	}

	files, err := scan.Readdir(-1)
	scan.Close()

	if err != nil {
		log.Println(err)
	} else {
		for _, file := range files {
			filename := file.Name()
			if filename[len(filename)-4:] == ".exe" {
				if err := os.Remove(filepath.Join(TempJobFolder, filename)); err != nil {
					log.Println(err)
				}
			}
		}
	}

	var retEnv []byte
	if work.ReturnEnv {
		retEnv = Hyades.ZipCompressFolder(TempJobFolder)
	}

	res.Env = retEnv
	res.StdOutStream = stdBuf.Bytes()
	res.ErrOutStream = errBuf.Bytes()
	resChan <- res
}
