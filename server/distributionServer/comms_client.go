package main

import (
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"

	"github.com/HeinOldewage/Hyades"
	"github.com/HeinOldewage/logicsocket"

	"github.com/HeinOldewage/Hyades/server/databaseDefinition"
)

type Client struct {
	Connected          bool
	Work               *databaseDefinition.Work
	ClientInfo         *databaseDefinition.ClientInfo
	RecievedClientInfo bool

	Log   *log.Logger
	Owner *WorkServer
}

const (
	WORK int32 = 1 + iota
	HEARTBEAT
)

func NewClient(ws *WorkServer) *Client {
	return &Client{Connected: true, ClientInfo: &databaseDefinition.ClientInfo{}, Work: nil, Log: ws.Log, Owner: ws}
}

func (c *Client) handle(conn net.Conn, ws *WorkServer) {
	defer conn.Close()

	reader := gob.NewDecoder(conn)

	c.ClientInfo = new(databaseDefinition.ClientInfo)
	err := reader.Decode(c.ClientInfo)
	if err != nil {
		c.FrameWorkError(err)
		return
	}
	c.Owner.Stats.Announced(c.ClientInfo)

	log.Println(c.ClientInfo.ComputerName, " Connected")

	c.Owner.Stats.Connected()
	defer c.Owner.Stats.Disconnected(c.ClientInfo)

	lc := logicsocket.Wrap(conn)

	workConn := lc.NewConnection(WORK)
	go c.ServiceWork(workConn)

	heartbeatConn := lc.NewConnection(HEARTBEAT)
	c.ServiceHeartBeat(heartbeatConn)

}

type nopCloser struct {
	io.Reader
	io.Writer
}

func NopCloser(rw io.ReadWriter) *nopCloser {
	return &nopCloser{rw, rw}
}

func (nopCloser) Close() error { return nil }

func (c *Client) ServiceWork(wr io.ReadWriter) {
	reader := gob.NewDecoder(wr)
	writer := gob.NewEncoder(wr)

	defer func() {
		if c.Work != nil {
			work := c.clearWork()
			var err string
			if e := recover(); e != nil {
				err = fmt.Sprint(e)
			}
			c.Owner.retryWork(work, "ServiceWork loop ended "+err)
		}
	}()

	for {
		job, work, err := c.Owner.getWork()
		if err != nil {
			log.Println(err)
			continue
		}
		log.Println("got work from job", work.PartOfID, "with id", work.PartID)
		c.Work = work

		work.Status = "Sending work to client " + c.ClientInfo.ComputerName
		work.CurrentClient = c.ClientInfo
		c.Owner.db.SaveWork(work)

		comms, err := CreateWorkComms(job, work)
		if err != nil {
			log.Println("ServiceWork writer.Encode(comms) error", err)
			c.FrameWorkError(err)
			return
		}
		err = writer.Encode(comms)
		if err != nil {
			log.Println("ServiceWork writer.Encode(comms) error", err)
			c.FrameWorkError(err)
			return
		}

		work.Status = "Awaiting response from client " + c.ClientInfo.ComputerName
		c.Owner.db.SaveWork(work)

		var res *Hyades.WorkResult = new(Hyades.WorkResult)
		err = reader.Decode(res)
		if err != nil {
			log.Println(res)
			log.Println("ServiceWork reader.Decode(res) error", err)
			n, err := io.CopyN(os.Stdout, wr, 1000)
			fmt.Println()
			log.Println(n, err)

			c.FrameWorkError(err)
			return
		}
		res.SetEnv(NopCloser(wr))

		c.clearWork()

		if res != nil && res.Error == "" {
			c.Owner.doneWork(job, work, res)
			c.Owner.Stats.DonePart(c.ClientInfo)
			c.Owner.db.JobDone(job.Id)
		} else {
			c.Owner.retryWork(work, res.Error)
			n, err := io.CopyN(ioutil.Discard, res.GetEnv(), int64(res.EnvLength)) // Discard the env
			log.Println("Got N", n, res.EnvLength, err)

			log.Println("ServiceWork ErrOutStream", res.ErrOutStream)
			log.Println("ServiceWork StdOutStream", res.StdOutStream)
			c.Owner.Log.Println("Client ", c.ClientInfo.ComputerName, "(", c.ClientInfo.OperatingSystem, ") terminated simulation with error:", res.Error)
			c.Owner.Stats.JobError()
		}

		err = c.Owner.db.SaveWork(work)
		if err != nil {
			log.Println("Saving work failed", err)
		} else {
			log.Println("Saved work", job.Name, work.Command, work.Parameters)
		}
	}
}

func (c *Client) ServiceHeartBeat(wr io.ReadWriter) {
	reader := gob.NewDecoder(wr)
	var beatAt *time.Time = new(time.Time)
	for {
		err := reader.Decode(beatAt)
		if err != nil {
			c.FrameWorkError(err)
			return
		}
		c.Owner.Log.Println("Client", c.ClientInfo.ComputerName, "heartbeat")
		log.Println("Client", c.ClientInfo.ComputerName, "heartbeat")
	}
}

func (c *Client) FrameWorkError(err error) {
	log.Println("Client ", c.ClientInfo.ComputerName, "(", c.ClientInfo.OperatingSystem, ") failed with error:", err)
	c.Owner.Log.Println("Client ", c.ClientInfo.ComputerName, "(", c.ClientInfo.OperatingSystem, ") failed with error:", err)
	c.Connected = false
	c.Owner.Stats.FrameWorkError()
}

func (c *Client) clearWork() *databaseDefinition.Work {
	work := c.Work
	c.Work = nil
	return work
}

func CreateWorkComms(j *databaseDefinition.Job, w *databaseDefinition.Work) (*Hyades.WorkComms, error) {

	res := Hyades.WorkComms{}
	file, err := os.Open(j.Env)
	if err != nil {
		return nil, err
	}
	res.Env, err = ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	res.ReturnEnv = j.ReturnEnv
	res.Parts.Command = w.Command
	res.Parts.Parameters = w.Parameters
	return &res, nil
}
