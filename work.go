package Hyades

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"

	"time"
)

/*
 All work is linked to a Job
*/
type Job struct {
	OwnerID int     `bson :"omitempty"`
	Id      int     `json:"id" bson:"_id,omitempty"`
	Parts   []*Work `bson :"omitempty"`

	JobFolder string
	//A friendly name to used in displays
	Name string

	//Path to env file
	Env       string `bson :"omitempty"`
	ReturnEnv bool   `bson :"omitempty"`

	WorkObservers *ObserverList `bson :"omitempty"`
}

func (j *Job) NumPartsDone() int32 {
	res := 0
	for _, part := range j.Parts {
		if part.Done {
			res++
		}
	}
	return int32(res)
}

func (j *Job) AddWork(w *Work) {
	j.Parts = append(j.Parts, w)
}

func NewJob(OwnerID int, JobID string, Parts []*Work, Env string) *Job {
	res := &Job{
		OwnerID:       OwnerID,
		Parts:         Parts,
		Env:           Env,
		WorkObservers: NewObserverList(),
	}
	return res
}

/*
A Work struct gives the Environment the Command must be executed and specifies whether the Environment must be returned
*/
type Work struct {
	partOf *Job
	PartID int32 `json:"id" bson:"_id,omitempty"`

	DispatchTime        time.Time
	FinishTime          time.Time
	TotalTimeDispatched time.Duration

	CompletedBy   *ClientInfo
	CurrentClient *ClientInfo
	Done          bool
	Dispatched    bool
	BeingHandled  bool
	FailCount     int
	Error         string
	Status        string

	Command    string
	Parameters []string
}

/*
A WorkState struct stores additional information about the Work, like its current state, how many times it has failed, etc
*/
type WorkComms struct {
	Env       []byte
	ReturnEnv bool
	Parts     struct {
		Command    string
		Parameters []string
	}
}

func NewWork(partOf *Job) *Work {
	work := &Work{
		partOf:    partOf,
		FailCount: 0,
		Done:      false,
		Status:    "",
	}
	partOf.Parts = append(partOf.Parts, work)

	return work
}

func (w *Work) Dispatch(ci *ClientInfo) {
	w.DispatchTime = time.Now()
	w.CurrentClient = ci
	w.Dispatched = true
	w.Save()
}

func (w *Work) Failed() {
	w.FinishTime = time.Now()
	w.TotalTimeDispatched = w.TotalTimeDispatched + (w.FinishTime.Sub(w.DispatchTime))
	w.FailCount++
	w.CurrentClient = nil
	w.Dispatched = false
	w.BeingHandled = false
	w.Save()
}

func (w *Work) Succeeded() error {
	w.FinishTime = time.Now()
	w.TotalTimeDispatched = w.TotalTimeDispatched + (w.FinishTime.Sub(w.DispatchTime))
	w.Done = true
	w.Dispatched = false
	w.BeingHandled = false
	w.CompletedBy = w.CurrentClient
	w.CurrentClient = nil
	return w.Save()
}

func (w *Work) SetStatus(status string) {
	w.Status = status
	log.Println("Setting status", status)
	w.Save()
}

func (w *Work) PartOf() *Job {
	return w.partOf
}

func (w *Work) Index() int {
	for i, p := range w.partOf.Parts {
		if p == w {
			return i
		}
	}
	return -1
}

func (w *Work) IsDone() bool {
	return w.Done
}

func (w *Work) RunTime() time.Duration {
	if w.DispatchTime.After(w.FinishTime) { //The work is actively being done by a client
		return time.Since(w.DispatchTime)
	} else {
		return w.FinishTime.Sub(w.DispatchTime) //Show time of last attempted run
	}
}

func (w *Work) TotalRunTime() time.Duration {
	if w.DispatchTime.After(w.FinishTime) { //The work is actively being done by a client
		return w.TotalTimeDispatched + time.Since(w.DispatchTime)
	} else {
		return w.TotalTimeDispatched
	}
}

func (w *Work) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	m["PartID"] = w.PartID
	m["Status"] = w.Status
	m["RunTime"] = w.RunTime().String()
	m["TotalRunTime"] = w.TotalRunTime().String()
	m["FailCount"] = w.FailCount
	b, e := json.Marshal(m)
	return b, e
}

func (w *Work) Save() error {
	return nil
}

func (j *Job) callback(e interface{}) {
	j.WorkObservers.Callback(e)
}

func (j *Job) MarshalBinary() (data []byte, err error) {
	res := &bytes.Buffer{}
	//Encode all the public stuff
	encoder := gob.NewEncoder(res)
	err = encoder.Encode(j.OwnerID)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(j.Id)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(j.Parts)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(j.NumPartsDone)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(j.Env)
	if err != nil {
		return nil, err
	}
	//We are not going to save observers (There is little sense as we cannot save events without dequeueing them)

	return res.Bytes(), nil
}

//Sets all private members for work and job to work correctly
func (j *Job) Setup() {
	for _, w := range j.Parts {
		w.partOf = j
	}
}

func (j *Job) Save() error {

	return nil
}

func (j *Job) CreateWorkComms(w *Work) (*WorkComms, error) {

	res := WorkComms{}
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

func (j *Job) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	decoder := gob.NewDecoder(r)
	res := decoder.Decode(j.OwnerID)
	if res != nil {
		return res
	}
	res = decoder.Decode(j.Id)
	if res != nil {
		return res
	}
	res = decoder.Decode(j.Parts)
	if res != nil {
		return res
	}
	res = decoder.Decode(j.NumPartsDone)
	if res != nil {
		return res
	}
	res = decoder.Decode(j.Env)
	if res != nil {
		return res
	}

	return nil
}

/*
The result of a doing Work
*/
type WorkResult struct {
	EnvLength    int
	env          io.ReadWriteCloser
	StdOutStream []byte
	ErrOutStream []byte
	Error        string
	Done         int32
}

func (wr *WorkResult) SetEnv(env io.ReadWriteCloser) {
	wr.env = env
}

func (wr *WorkResult) GetEnv() io.ReadWriteCloser {
	return wr.env
}

type ClientInfo struct {
	OperatingSystem string
	ComputerName    string
}

func NewClientInfo() *ClientInfo {
	return &ClientInfo{"OPERATING_SYSTEM", "COMPUTER_NAME"}
}

type Tag int

const (
	Info Tag = iota
	Ready
	Done
	Heartbeat
)
