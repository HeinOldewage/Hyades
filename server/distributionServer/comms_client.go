package main

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/HeinOldewage/logicsocket"
	"github.com/HeinOldewage/Hyades"
)

type Client struct {
	Connected          bool
	Work               *Hyades.Work
	ClientInfo         *Hyades.ClientInfo
	RecievedClientInfo bool

	Log   *log.Logger
	Owner *WorkServer
}

const (
	WORK int32 = 1 + iota
	HEARTBEAT
)

func NewClient(ws *WorkServer) *Client {
	return &Client{Connected: true, ClientInfo: Hyades.NewClientInfo(), Work: nil, Log: ws.Log, Owner: ws}
}

func (c *Client) handle(conn net.Conn, ws *WorkServer) {
	defer conn.Close()

	reader := gob.NewDecoder(conn)

	c.ClientInfo = new(Hyades.ClientInfo)
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
		work, err := c.Owner.getWork()
		if err != nil {
			log.Println(err)
			continue
		}
		log.Println("got work from job", work.PartOf().Id)
		c.Work = work

		work.SetStatus("Sending work to client " + c.ClientInfo.ComputerName)
		work.Dispatch(c.ClientInfo)

		comms, err := work.PartOf().CreateWorkComms(work)
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

		work.SetStatus("Awaiting response from client " + c.ClientInfo.ComputerName)

		var res *Hyades.WorkResult = new(Hyades.WorkResult)
		err = reader.Decode(res)
		if err != nil {
			log.Println("ServiceWork reader.Decode(res) error", err)
			c.FrameWorkError(err)
			return
		}
		res.SetEnv(NopCloser(wr))

		c.clearWork()

		if res != nil && res.Error == "" {
			c.Owner.doneWork(work, res)
			c.Owner.Stats.DonePart(c.ClientInfo)
		} else {
			c.Owner.retryWork(work, res.Error)
			log.Println("ServiceWork ErrOutStream", res.ErrOutStream)
			log.Println("ServiceWork StdOutStream", res.StdOutStream)
			c.Owner.Log.Println("Client ", c.ClientInfo.ComputerName, "(", c.ClientInfo.OperatingSystem, ") terminated simulation with error:", res.Error)
			c.Owner.Stats.JobError()
		}

		err = c.Owner.db.SaveWork(work)
		if err != nil {
			log.Println("Saving work failed", err)
		} else {
			log.Println("Saved work", work.PartOf().Name, work.Command, work.Parameters)
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

func (c *Client) clearWork() *Hyades.Work {
	work := c.Work
	c.Work = nil
	return work
}
