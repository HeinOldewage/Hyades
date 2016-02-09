package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
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
	defer conn.Close()
	job := ts.DBhandle.GetNextJob()
	log.Println("Got a job", job)
	data, err := json.Marshal(job)

	if err != nil {
		log.Println(err)
		return
	}
	conn.Write(data)
}

func main() {
	fmt.Println("This is the distribution server")
	db, err := NewDB()
	if err != nil {
		log.Println(err)
	}
	ts := TaurusServer{ListenAddress: ":8085", DBhandle: db}
	ts.Listen()

}
