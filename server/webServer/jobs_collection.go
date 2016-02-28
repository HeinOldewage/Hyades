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
	err = jm.session.DB("Admin").C("Jobs").Find(&jobIn).One(job)

	return job, err
}

func (jm *JobMap) GetAll() (jobs []*Hyades.Job, err error) {
	iterator := jm.session.DB("Admin").C("Jobs").Find(nil).Iter()
	var job Hyades.Job
	for iterator.Next(&job) {
		jobs = append(jobs, &job)
	}

	return jobs, iterator.Err()
}

func (jm *JobMap) AddJob(job *Hyades.Job) error {
	return jm.session.DB("Admin").C("Jobs").Insert(job)
}
