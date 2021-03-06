package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	//	"strings"

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

var ServerAddress0 *string = flag.String("ServerAddress", "127.0.0.1:8080", "The server to get Jobs from")
var ServerAddress1 *string = flag.String("ServerAddress1", "", "The backup server to get Jobs from")
var logFile *string = flag.String("log", "", "The File to log to, if blank logging is done to stdout")
var retryTime *int = flag.Int("retry", 6, "The client will attempt to connect to the server approximately every so many seconds")
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

type nopCloser struct {
	io.Reader
	io.Writer
}

func (nopCloser) Close() error { return nil }

func DoWork(work *Hyades.WorkComms, resChan chan *Hyades.WorkResult) {

	res := &Hyades.WorkResult{EnvLength: 0, StdOutStream: make([]byte, 0), ErrOutStream: make([]byte, 0), Error: "", Done: 0}

	defer func() {
		if err := recover(); err != nil {
			res.Error = fmt.Sprint(err)
		}
		resChan <- res
	}()
	TempJobFolder := filepath.Join("Env", "Temp")
	log.Println("Making folder:[", TempJobFolder, "]")

	err := os.MkdirAll(TempJobFolder, os.ModeDir|os.ModePerm)
	if err != nil {
		log.Println("Error creating folder:", err)
		res.Error = err.Error()

		return
	}
	defer os.RemoveAll(TempJobFolder)

	envreader := bytes.NewReader(work.Env)
	unzipper, err := zip.NewReader(envreader, int64(len(work.Env)))

	if err != nil {
		log.Println("Error zip.NewReader:", err)
		res.Error = "Error zip.NewReader:" + err.Error()
		return
	}

	for _, file := range unzipper.File {
		os.MkdirAll(filepath.Join(TempJobFolder, path.Dir(file.Name)), os.ModeDir|os.ModePerm)

		outfile, err := os.Create(filepath.Join(TempJobFolder, file.Name))
		if err != nil {
			log.Println("Error creating file from zip", err)
			res.Error = "Error creating file from zip" + err.Error()
			return
		}
		zf, err := file.Open()
		if err != nil {
			log.Println("Error reading zip:", err)
			res.Error = "Error reading zip:" + err.Error()
			return
		}
		_, err = io.Copy(outfile, zf)
		if err != nil {
			log.Println("Error writing zip contents to file", err)
			res.Error = "Error writing zip contents to file" + err.Error()
			return
		}
		outfile.Close()
		zf.Close()
	}

	stdBuf := new(bytes.Buffer)
	errBuf := new(bytes.Buffer)

	log.Println("Chmoding ", filepath.Join(TempJobFolder, work.Parts.Command))
	err = os.Chmod(filepath.Join(TempJobFolder, work.Parts.Command), os.ModePerm)
	if err != nil {
		log.Println("Chmod", err)
	}

	var cmd *exec.Cmd
	if runtime.GOOS == "linux" {
		log.Println("Setting up the linux ccommand", work.Parts.Command, work.Parts.Parameters)
		cmd = exec.Command(work.Parts.Command, work.Parts.Parameters...)
	} else {
		log.Println("Setting up the windows ccommand")
		cmd = exec.Command(work.Parts.Command, work.Parts.Parameters...)
	}

	fullpath, _ := filepath.Abs(TempJobFolder)
	fullpath = "\"" + fullpath + "\""

	log.Println("Running command", work.Parts.Command)

	cmd.Dir = TempJobFolder
	cmd.Stdout = stdBuf
	cmd.Stderr = errBuf
	err = cmd.Run()
	log.Println("Std", len(stdBuf.Bytes()))
	log.Println("Err", len(errBuf.Bytes()))
	if err != nil {
		if _, ok := err.(*exec.ExitError); !ok {

			log.Println("Error running command:", err)
			res.Error = "Error running command:" + err.Error()
		}
	}
	cmd.StdoutPipe()
	log.Println("Done command", work.Parts.Command)

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

	var tempWriter io.ReadWriteCloser
	var Bufferlength func() int
	tempFile, err := os.Create("temp.zip")
	if err == nil {
		tempWriter = tempFile

		Bufferlength = func() int {
			s, err := tempFile.Stat()
			if err != nil {
				panic(err)
			}
			return int(s.Size())
		}

	} else {
		log.Println("Could not keep zip on disk, trying to use memory")
		buffer := &bytes.Buffer{}
		Bufferlength = func() int { return int(buffer.Len()) }
		tempWriter = nopCloser{buffer, buffer}
	}

	if work.ReturnEnv {
		Hyades.ZipCompressFolderWriter(TempJobFolder, tempWriter)
	}

	if tempFile != nil {
		tempFile.Seek(0, 0)
	}

	res.SetEnv(tempWriter)
	res.EnvLength = Bufferlength()
	res.StdOutStream = stdBuf.Bytes()
	res.ErrOutStream = errBuf.Bytes()
	log.Println("Did work:", res)

}
