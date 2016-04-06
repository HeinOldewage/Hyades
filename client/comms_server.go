package main

import (
	"encoding/gob"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	"bitbucket.org/Neoin/logicsocket"
	"github.com/HeinOldewage/Hyades"
)

type Server struct {
	conn net.Conn

	reader *gob.Decoder
	writer *gob.Encoder
}

const (
	WORK int32 = 1 + iota
	HEARTBEAT
)

func StartComms(address string, secondaryAddress string) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	log.Println("Trying server address", address)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Println(err)
		log.Println("Trying server address", secondaryAddress)
		conn, err = net.Dial("tcp", secondaryAddress)
		if err != nil {
			log.Println(err)
			return
		}
	}
	defer conn.Close()

	var ci *Hyades.ClientInfo = new(Hyades.ClientInfo)
	log.Println("About to request computer name")

	ci.ComputerName, err = os.Hostname()
	if err != nil {
		if runtime.GOOS == "windows" {
			ci.ComputerName = os.Getenv("COMPUTERNAME")
		}
	}
	ci.OperatingSystem = runtime.GOOS
	log.Println("Acquired computer name; about to send client info")

	connwriter := gob.NewEncoder(conn)
	err = connwriter.Encode(ci)
	if err != nil {
		log.Println("connwriter", err)
		return
	}

	lc := logicsocket.Wrap(conn)

	workConn := lc.NewConnection(WORK)
	log.Println("Starting Work Serivce")
	go ServiceWork(workConn)

	heartbeatConn := lc.NewConnection(HEARTBEAT)
	log.Println("Starting HeartBeat")
	SericeHeartBeat(heartbeatConn)
}

func ServiceWork(wr io.ReadWriter) {
	reader := gob.NewDecoder(wr)
	writer := gob.NewEncoder(wr)

	var work *Hyades.WorkComms = new(Hyades.WorkComms)
	workResults := make(chan *Hyades.WorkResult)
	for {
		err := reader.Decode(work)
		if err != nil {
			log.Println("ServiceWork Decode", err)
			return
		}
		go DoWork(work, workResults)

		res := <-workResults
		log.Printf("DoWork done %T %v \n", res, res)
		err = writer.Encode(res)
		if err != nil {
			log.Println("ServiceWork Encode", err)
			return
		}
		defer res.GetEnv().Close()
		_, err = io.CopyN(wr, res.GetEnv(), int64(res.EnvLength))

		if err != nil {
			log.Println("ServiceWork CopyN", err)
			return
		}
		log.Println("ServiceWork sent back")
	}
}

func SericeHeartBeat(wr io.ReadWriter) {
	writer := gob.NewEncoder(wr)

	for {
		time.Sleep(time.Duration(*heartbeat) * time.Second)
		if writer.Encode(time.Now()) != nil {
			return
		}
	}
}
