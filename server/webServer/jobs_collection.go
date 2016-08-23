package main

import (
	"database/sql"

	"github.com/HeinOldewage/Hyades"

	_ "github.com/mattn/go-sqlite3"
)

type JobMap struct {
	dbFile string
}

func NewJobMap(dbFile string) *JobMap {
	return &JobMap{dbFile}
}

func (jm *JobMap) NewJob(user *Hyades.Person) *Hyades.Job {
	return &Hyades.Job{OwnerID: user.Id}
}

func (jm *JobMap) GetJob(id string) (job *Hyades.Job, err error) {
	conn, err := sql.Open("sqlite3", jm.dbFile)
	if err != nil {
		return nil, err
	}
	res, err := conn.Query("Select * from JOBS where ID = ?", id)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	job = new(Hyades.Job)
	if res.Next() {
		res.Scan(&job.Id, &job.OwnerID, &job.Name, &job.Env, &job.ReturnEnv)
	}

	return job, err
}

func (jm *JobMap) GetAll(user *Hyades.Person) (jobs []*Hyades.Job, err error) {
	conn, err := sql.Open("sqlite3", jm.dbFile)
	if err != nil {
		return nil, err
	}
	res, err := conn.Query("Select * from JOBS where OwnerID = ?", user.Id)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Next() {
		job := &Hyades.Job{}
		res.Scan(&job.Id, &job.OwnerID, &job.Name, &job.Env, &job.ReturnEnv)
		jobs = append(jobs, job)
	}

	return jobs, res.Err()
}

func (jm *JobMap) AddJob(job *Hyades.Job) error {
	conn, err := sql.Open("sqlite3", jm.dbFile)
	if err != nil {
		return err
	}
	res, err := conn.Exec("Insert into JOBS (id,OwnerID,Name,Env,ReturnEnv) values ( ? , ? , ? , ? , ? );", job.Id, &job.OwnerID, &job.Name, &job.Env, &job.ReturnEnv)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		job.Id = int(id)
	}

	return nil
}
