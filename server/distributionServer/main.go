package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/HeinOldewage/Hyades"
)

type TaurusServer struct {
	ListenAddress string
	DBhandle      *DB
}

func (ts *TaurusServer) Listen() error {
	listener, err := net.Listen("tcp", ts.ListenAddress)
	if err != nil {
		return err
	}
	defer listener.Close()
	for {
		conn, _ := listener.Accept()
		log.Println("Got connection")
		go ts.handle(conn)
	}
}

func (ts *TaurusServer) handle(conn net.Conn) {
	/*
		Must:
			Get a job
			Send the job
			Wait for job to be recieved
			Save the results
		Later:
			Allow for job cancelation (If it is stuck in an infinite loop
			Allow reconnection of clients(network failure or server restart)
	*/
	defer conn.Close()
	job := ts.DBhandle.GetNextJob()
	log.Println("Got a job", job)

	var sendJob Hyades.Job
	sendJob.Command = job.Works.Jobs.Command
	sendJob.SaveEnvironment = job.Works.Jobs.SaveEnvironment

	envFile, err := os.Open(job.Works.Env)
	if err != nil {
		log.Println(err)
		return
	}
	sendJob.Env, err = ioutil.ReadAll(envFile)
	if err != nil {
		log.Println(err)
		return
	}

	data, err := json.Marshal(sendJob)

	if err != nil {
		log.Println(err)
		return
	}
	conn.Write(data)

	jsonReader := json.NewDecoder(conn)

	jobResult := Hyades.JobResult{}
	jsonReader.Decode(&jobResult)

	log.Println(jobResult)

}

func main() {
	fmt.Println("This is the distribution server")
	db, err := NewDB()
	if err != nil {
		log.Println(err)
	}
	log.Println("Job:", db.GetNextJob())
	ts := TaurusServer{ListenAddress: ":8085", DBhandle: db}

	ts.Listen()

}
