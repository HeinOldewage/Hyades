package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"

	"github.com/HeinOldewage/Hyades"
)

type TaurusClient struct {
	ServerAddress string
	conn          net.Conn
}

func (tc *TaurusClient) Connect() (err error) {
	tc.conn, err = net.Dial("tcp", tc.ServerAddress)
	return err
}

func (tc *TaurusClient) GetJob() (job *Hyades.Job, err error) {
	decoder := json.NewDecoder(tc.conn)
	job = new(Hyades.Job)
	err = decoder.Decode(job)
	return
}

func (tc *TaurusClient) doJob(job *Hyades.Job) (*Hyades.JobResult, error) {
	jobResult := new(Hyades.JobResult)
	//Setup The Environment
	zipFile, err := zip.NewReader(bytes.NewReader(job.Env), int64(len(job.Env)))
	if err != nil {
		return nil, err
	}
	TempJobFolder := "Env"
	for _, f := range zipFile.File {
		os.MkdirAll(filepath.Join(TempJobFolder, path.Dir(f.Name)), os.ModeDir)
		outFile, err := os.Create(f.Name)
		if err != nil {
			return nil, err
		}
		rc, err := f.Open()
		if err != nil {
			log.Fatal(err)
		}
		_, err = io.Copy(outFile, rc)
		if err != nil {
			log.Println(err)
		}
		rc.Close()
		outFile.Close()
	}
	//Run the command
	var cmd *exec.Cmd
	if runtime.GOOS == "linux" {
		cmdstr := job.Command
		cmd = exec.Command("sh", "-c", "cd "+TempJobFolder, " && "+cmdstr)
	}
	stdBuf := new(bytes.Buffer)
	errBuf := new(bytes.Buffer)

	cmd.Stdout = stdBuf
	cmd.Stderr = errBuf
	err = cmd.Run()

	//Collect the environment

	jobResult.StdOut = stdBuf.Bytes()
	jobResult.StdErr = errBuf.Bytes()
	jobResult.SystemError = err.Error()
	if job.SaveEnvironment {
		jobResult.Env = Hyades.ZipCompressFolder(TempJobFolder)
	}
	return nil, nil
}

func (tc *TaurusClient) SendJobResult(jobResult *Hyades.JobResult) (err error) {
	encoder := json.NewEncoder(tc.conn)

	err = encoder.Encode(jobResult)
	return err
}

func main() {
	fmt.Println("This is the taurus client")

	tc := TaurusClient{ServerAddress: "127.0.0.1:8085"}

	err := tc.Connect()
	if err != nil {
		log.Println(err)
		return
	}
	for {
		job, err := tc.GetJob()
		if err != nil {
			log.Println(err)
		}
		jobResult, err := tc.doJob(job)
		if err != nil {
			log.Println(err)
		}

		err = tc.SendJobResult(jobResult)
		if err != nil {
			log.Println(err)
		}
	}

}
