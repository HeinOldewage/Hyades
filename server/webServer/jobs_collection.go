package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"path/filepath"

	"github.com/HeinOldewage/Hyades"

	_ "github.com/mattn/go-sqlite3"
)

type JobMap struct {
	session *sql.DB
	dbFile  string
}

func NewJobMap(dbFile string) *JobMap {
	res := &JobMap{dbFile: dbFile}
	conn, err := sql.Open("sqlite3", "file:"+dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return nil
	}
	res.session = conn
	return res
}

func (jm *JobMap) NewJob(user *Hyades.Person) *Hyades.Job {
	return &Hyades.Job{OwnerID: user.Id}
}

func (jm *JobMap) GetJob(id string) (job *Hyades.Job, err error) {
	conn, err := sql.Open("sqlite3", "file:"+jm.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	res, err := conn.Query("Select * from JOBS where ID = ?", id)
	if err != nil {
		return nil, err
	}
	defer closeQuery(res)
	job = new(Hyades.Job)
	if res.Next() {
		res.Scan(&job.Id, &job.OwnerID, &job.Name, &job.JobFolder, &job.Env, &job.ReturnEnv)
	}

	log.Println("JobID", job.Id)

	partres, err := conn.Query("Select Id,DispatchTime,FinishTime,TotalTimeDispatched,Done,Dispatched,BeingHandled,FailCount,Error,Status,Command from JOBPARTS where OwnerID = ?", job.Id)
	if err != nil {
		log.Println(err)
	}
	defer closeQuery(partres)
	for partres.Next() {
		var part *Hyades.Work = Hyades.NewWork(job)

		err := partres.Scan(&part.PartID, &part.DispatchTime, &part.FinishTime, &part.TotalTimeDispatched, &part.Done, &part.Dispatched, &part.BeingHandled, &part.FailCount, &part.Error, &part.Status, &part.Command)
		if err != nil {
			log.Println("partres.Scan", err)
		}

		log.Println("PartId", part.PartID)

		paramres, err := conn.Query("Select Parameters from Parameters where JOBPARTSID = ?", part.PartID)
		defer closeQuery(paramres)
		for paramres.Next() {
			var param string
			err := paramres.Scan(&param)
			if err != nil {
				log.Println("paramres.Scan", err)
			}
			part.Parameters = append(part.Parameters, param)
		}
	}

	if partres.Err() != nil {
		log.Println(partres.Err())
	}

	log.Println("Job has", len(job.Parts), " parts")

	return job, err
}

func (jm *JobMap) GetAll(user *Hyades.Person) (jobs []*Hyades.Job, err error) {
	conn, err := sql.Open("sqlite3", "file:"+jm.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	log.Println("Getting job for user", user.Username, user.Id)
	res, err := conn.Query("Select * from JOBS where OwnerID = ?", user.Id)
	if err != nil {
		return nil, err
	}
	defer closeQuery(res)

	for res.Next() {
		job := &Hyades.Job{}
		err := res.Scan(&job.Id, &job.OwnerID, &job.Name, &job.JobFolder, &job.Env, &job.ReturnEnv)
		if err != nil {
			log.Println(err)
		}
		log.Println("Job", job.Id, "Belongs to id", job.OwnerID)
		partres, err := conn.Query("Select Id,DispatchTime,FinishTime,TotalTimeDispatched,Done,Dispatched,BeingHandled,FailCount,Error,Status,Command from JOBPARTS where OwnerID = ?", job.Id)
		if err != nil {
			log.Println(err)
		}
		defer closeQuery(partres)
		for partres.Next() {
			var part Hyades.Work

			err := partres.Scan(&part.PartID, &part.DispatchTime, &part.FinishTime, &part.TotalTimeDispatched, &part.Done, &part.Dispatched, &part.BeingHandled, &part.FailCount, &part.Error, &part.Status, &part.Command)
			if err != nil {
				log.Println("partres.Scan", err)
			}

			paramres, err := conn.Query("Select Parameters from Parameters where JOBPARTSID = ?", part.PartID)
			defer closeQuery(paramres)
			for paramres.Next() {
				var param string
				err := paramres.Scan(&param)
				if err != nil {
					log.Println("paramres.Scan", err)
				}
				part.Parameters = append(part.Parameters, param)
			}

		}
		if partres.Err() != nil {
			log.Println(partres.Err())
		}

		jobs = append(jobs, job)
	}

	return jobs, res.Err()
}

func closeQuery(conn *sql.Rows) {
	conn.Close()
}

func (jm *JobMap) AddJob(job *Hyades.Job) error {
	conn, err := sql.Open("sqlite3", "file:"+jm.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return err
	}
	defer conn.Close()
	trans, err := conn.Begin()
	if err != nil {
		return err
	}
	defer trans.Rollback()

	res, err := trans.Exec("Insert into JOBS (OwnerID,Name,JobFolder,Env,ReturnEnv) values (  ? , ? , ? , ? , ? );", &job.OwnerID, &job.Name, &job.JobFolder, &job.Env, &job.ReturnEnv)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err == nil {
		job.Id = int(id)
		log.Println("Got id back")
	} else {
		log.Println("Cannot get job ID", err)
	}

	for _, part := range job.Parts {
		//JOBPARTS (Id,OwnerID,DispatchTime,FinishTime,TotalTimeDispatched,CompletedBy,CurrentClient,Done,Dispatched,BeingHandled,FailCount,Error,Status,Command)
		res, err := trans.Exec("Insert into JOBPARTS (OwnerID,DispatchTime,FinishTime,TotalTimeDispatched,CompletedBy,CurrentClient,Done,Dispatched,BeingHandled,FailCount,Error,Status,Command)"+
			" values (  ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?);", id, part.DispatchTime, part.FinishTime, part.TotalTimeDispatched, 0, 0, part.Done, part.Dispatched, part.BeingHandled, part.FailCount, part.Error, part.Status, part.Command)
		if err != nil {
			return err
		}

		partid, err := res.LastInsertId()
		if err == nil {
			part.PartID = int32(partid)
		}

		for _, param := range part.Parameters {
			_, err := trans.Exec("Insert into Parameters (JOBPARTSID,Parameters) values (  ? , ?  );", part.PartID, param)
			if err != nil {
				return err
			}
		}

	}

	trans.Commit()
	return nil
}

func (jm *JobMap) Delete(job *Hyades.Job, datapath string) error {
	os.RemoveAll(job.Env)

	os.RemoveAll(filepath.Join(datapath, job.JobFolder, job.Name+fmt.Sprint(job.Id)))
	os.RemoveAll(filepath.Join(datapath, job.JobFolder, "Job"+job.Name+fmt.Sprint(job.Id)+".zip"))
	conn, err := sql.Open("sqlite3", "file:"+jm.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return err
	}
	defer conn.Close()
	trans, err := conn.Begin()
	if err != nil {
		return err
	}
	defer trans.Rollback()

	_, err = trans.Exec("delete from JOBS where ID = ? ;", &job.Id)
	if err != nil {
		return err
	}

	for _, part := range job.Parts {

		_, err := trans.Exec("delete from JOBPARTS where OwnerID = ? ;", &job.Id)
		if err != nil {
			return err
		}

		_, err = trans.Exec("delete from Parameters where JOBPARTSID = ?  ;", part.PartID)
		if err != nil {
			return err
		}

	}

	trans.Commit()
	return nil

}
