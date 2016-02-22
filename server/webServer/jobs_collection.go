package main

import (
	"github.com/HeinOldewage/Hyades"
	"fmt"
	"sync"
	"sync/atomic"
)

type JobMap struct {
	JobID   int32
	Jobs    map[string]*Hyades.Job
	jobLock *sync.RWMutex
	JobObservers *Hyades.ObserverList
}

func NewJobMap() *JobMap {
	return &JobMap{
		0,
		make(map[string]*Hyades.Job),
		&sync.RWMutex{},
		Hyades.NewObserverList(),
		
	}
}

func (jm *JobMap) NewJob(user *Hyades.Person) *Hyades.Job {
	return Hyades.NewJob(user, fmt.Sprint(atomic.AddInt32(&jm.JobID, 1)), make([]*Hyades.Work, 0), 0, make([]byte, 0))
}

func (jm *JobMap) GetJob(id string) (job *Hyades.Job, ok bool) {
	jm.jobLock.RLock()
	defer jm.jobLock.RUnlock()
	job, ok = jm.Jobs[id]
	return
}

func (jm *JobMap) GetAll() (jobs []*Hyades.Job) {
	jm.jobLock.RLock()
	defer jm.jobLock.RUnlock()
	jobs = make([]*Hyades.Job, 0)
	for _, job := range jm.Jobs {
		jobs = append(jobs, job)
	}

	return
}

func (jm *JobMap) AddJob(job *Hyades.Job) {
	jm.jobLock.RLock()
	defer jm.jobLock.RUnlock()
	jm.Jobs[job.JobID] = job
	jm.JobObservers.Callback(job)
}
