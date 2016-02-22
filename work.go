package Hyades

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"time"
)

/*
 All work is linked to a Job
*/
type Job struct {
	Owner        *Person
	JobID        string
	Parts        []*Work
	NumPartsDone int32

	Env []byte

	WorkObservers *ObserverList
}

func (j *Job) AddWork(w *Work) {
	j.Parts = append(j.Parts, w)
}

func NewJob(Owner *Person, JobID string, Parts []*Work, NumPartsDone int32, Env []byte) *Job {
	res := &Job{
		Owner:         Owner,
		JobID:         JobID,
		Parts:         Parts,
		NumPartsDone:  NumPartsDone,
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
	PartID string

	DispatchTime        time.Time
	FinishTime          time.Time
	TotalTimeDispatched time.Duration

	CompletedBy   *ClientInfo
	CurrentClient *ClientInfo
	Done          bool
	failCount     int
	status        string

	WindowsCommand string
	LinuxCommand   string
	ReturnEnv      bool
}

/*
A WorkState struct stores additional information about the Work, like its current state, how many times it has failed, etc
*/
type WorkComms struct {
	Env            []byte
	WindowsCommand string
	LinuxCommand   string
	ReturnEnv      bool
	PartID         string
}

func NewWork(partOf *Job, partId string, windowsCmd, linuxCmd string, returnEnv bool) *Work {
	return &Work{
		partOf:         partOf,
		PartID:         partId,
		WindowsCommand: windowsCmd,
		LinuxCommand:   linuxCmd,
		ReturnEnv:      returnEnv,
		failCount:      0,
		Done:           false,
		status:         "",
	}
}

func (w *Work) Dispatched(ci *ClientInfo) {
	w.DispatchTime = time.Now()
	w.CurrentClient = ci
	w.partOf.callback(w)
}

func (w *Work) Failed() {
	w.FinishTime = time.Now()
	w.TotalTimeDispatched = w.TotalTimeDispatched + (w.FinishTime.Sub(w.DispatchTime))
	w.failCount++
	w.CurrentClient = nil
	w.partOf.callback(w)
}

func (w *Work) Succeeded() {
	w.FinishTime = time.Now()
	w.TotalTimeDispatched = w.TotalTimeDispatched + (w.FinishTime.Sub(w.DispatchTime))
	w.Done = true
	w.CompletedBy = w.CurrentClient
	w.CurrentClient = nil
	w.partOf.callback(w)
}

func (w *Work) SetStatus(status string) {
	w.status = status
	w.partOf.callback(w)
}

func (w *Work) PartOf() *Job {
	return w.partOf
}

func (w *Work) IsDone() bool {
	return w.Done
}

func (w *Work) Status() string {
	return w.status
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

func (w *Work) FailCount() int {
	return w.failCount
}

func (w *Work) MakeComms() *WorkComms {
	return &WorkComms{
		//PartID:         w.PartID,
		WindowsCommand: w.WindowsCommand,
		LinuxCommand:   w.LinuxCommand,
		ReturnEnv:      w.ReturnEnv,
		Env:            w.partOf.Env,
		PartID:         w.PartID,
	}
}

func (w *Work) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	m["PartID"] = w.PartID
	m["Status"] = w.Status()
	m["RunTime"] = w.RunTime().String()
	m["TotalRunTime"] = w.TotalRunTime().String()
	m["FailCount"] = w.FailCount()
	b,e := json.Marshal(m)
	return b,e
}

func (j *Job) callback(e interface{}) {
	j.WorkObservers.Callback(e)
}

func (j *Job) MarshalBinary() (data []byte, err error) {
	res := &bytes.Buffer{}
	//Encode all the public stuff
	encoder := gob.NewEncoder(res)
	err = encoder.Encode(j.Owner)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(j.JobID)
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

func (j *Job) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	decoder := gob.NewDecoder(r)
	res := decoder.Decode(j.Owner)
	if res != nil {
		return res
	}
	res = decoder.Decode(j.JobID)
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

	for _, w := range j.Parts {
		w.partOf = j
	}

	return nil
}

/*
The result of a doing Work
*/
type WorkResult struct {
	Env          []byte
	StdOutStream []byte
	ErrOutStream []byte
	Error        string
	Done         int32
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
