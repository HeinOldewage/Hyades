package main

import (
	"github.com/HeinOldewage/Hyades"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type JobMap struct {
	session *mgo.Session
}

func NewJobMap(session *mgo.Session) *JobMap {
	return &JobMap{session}
}

func (jm *JobMap) NewJob(user *Hyades.Person) *Hyades.Job {
	return &Hyades.Job{OwnerID: user.Id}
}

func (jm *JobMap) GetJob(id string) (job *Hyades.Job, err error) {
	jobIn := make(map[string]interface{})
	jobIn["_id"] = bson.ObjectId(id)
	job = new(Hyades.Job)
	err = jm.session.DB("Hyades").C("Jobs").Find(&jobIn).One(job)

	return job, err
}

func (jm *JobMap) GetAll(user *Hyades.Person) (jobs []*Hyades.Job, err error) {
	find := make(map[string]interface{})
	find["ownerid"] = user.Id
	iterator := jm.session.DB("Hyades").C("Jobs").Find(find).Iter()
	var job *Hyades.Job = new(Hyades.Job)
	for iterator.Next(job) {
		jobs = append(jobs, job)
		job = new(Hyades.Job)
	}

	return jobs, iterator.Err()
}

func (jm *JobMap) AddJob(job *Hyades.Job) error {
	return jm.session.DB("Hyades").C("Jobs").Insert(job)
}
