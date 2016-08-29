package main

import (
	"fmt"
	"log"
	"time"

	"github.com/HeinOldewage/Hyades"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	conn   *sql.DB
	dbFile string
}

func NewDB(DBFile string) (*DB, error) {

	conn, err := sql.Open("sqlite3", "file:"+DBFile+"?_loc=auto")
	if err != nil {
		return nil, err
	}

	res := &DB{dbFile: DBFile, conn: conn}
	err = res.initDB()

	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *DB) GetNextJob() (work *Hyades.Work, err error) {
	//transaction this
	var sleepCounter int = 0
	var jobId int
	for {
		tx, err := db.conn.Begin()
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		res, err := tx.Query("Select OwnerID,Id,DispatchTime,FinishTime,TotalTimeDispatched,Done,Dispatched,BeingHandled,FailCount,Error,Status,Command from JOBPARTS where ((BeingHandled = ? ) and (Dispatched = ? ) and (Done = ? )) limit 1; ", false, false, false)
		if err != nil {
			log.Println("Could not select a job", err)
			tx.Rollback()
			return nil, err
		}
		work = new(Hyades.Work)

		if res.Next() {
			err := res.Scan(&jobId,
				&work.PartID,
				&work.DispatchTime,
				&work.FinishTime,
				&work.TotalTimeDispatched,
				&work.Done,
				&work.Dispatched,
				&work.BeingHandled,
				&work.FailCount,
				&work.Error,
				&work.Status,
				&work.Command)
			if err != nil {
				log.Println("Could not scan a job", err)
				tx.Rollback()
				return nil, err
			}

			if work.BeingHandled {
				log.Println("Work is already being handled")
			}
		} else {
			tx.Commit()
			if sleepCounter < 60 {
				sleepCounter++
			}
			time.Sleep(time.Second * time.Duration(sleepCounter))
			continue
		}

		ures, err := tx.Exec("UPDATE JOBPARTS  set BeingHandled = ? where Id = ?; ", true, work.PartID)
		if err != nil {
			log.Println("Could not update a job", err)
			tx.Rollback()
			return nil, err
		}

		i, _ := ures.RowsAffected()
		if i != 1 {
			log.Println("Updating did not affect one row", i)
		}
		err = tx.Commit()
		if err != nil {
			log.Println("Could not commit job chekout", err)
		}
		break
	}
	log.Println("Got a jobpart with id", work.PartID)
	job, err := db.GetJob(jobId)
	if err != nil {
		log.Println(err)
	}

	for _, part := range job.Parts {
		if part.PartID == work.PartID {
			if part.BeingHandled {
				log.Println("Part is being handles after loading")
			}
			return part, nil
		}
	}

	return nil, fmt.Errorf("Could not find work in job %i", work.PartID)
}

func (db *DB) GetCurrentClientID(c *Hyades.ClientInfo) (int, error) {
	if c == nil {
		return 0, nil
	}
	res, err := db.conn.Query("Select Id from CurrentClient where OperatingSystem = ? and ComputerName = ? ; ", c.OperatingSystem, c.ComputerName)
	if err != nil {
		return 0, err
	}
	var id int64
	if res.Next() {
		res.Scan(&id)
	} else {
		res, err := db.conn.Exec("Insert into CurrentClient (OperatingSystem,ComputerName) values(? , ?);", c.OperatingSystem, c.ComputerName)
		if err != nil {
			return 0, err
		}
		id, _ = res.LastInsertId()
	}
	return int(id), nil
}

func (db *DB) GetJob(id int) (job *Hyades.Job, err error) {
	conn, err := sql.Open("sqlite3", "file:"+db.dbFile+"?_loc=auto")
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
		res.Scan(&job.Id, &job.OwnerID, &job.Name, &job.JobFolder, &job.Env, &job.ReturnEnv)
	}

	partres, err := conn.Query("Select Id,DispatchTime,FinishTime,TotalTimeDispatched,Done,Dispatched,BeingHandled,FailCount,Error,Status,Command from JOBPARTS where OwnerID = ?", job.Id)
	if err != nil {
		log.Println(err)

	}
	for partres.Next() {
		var part *Hyades.Work = Hyades.NewWork(job)

		err := partres.Scan(&part.PartID, &part.DispatchTime, &part.FinishTime, &part.TotalTimeDispatched, &part.Done, &part.Dispatched, &part.BeingHandled, &part.FailCount, &part.Error, &part.Status, &part.Command)
		if err != nil {
			log.Println("partres.Scan", err)
		}
		paramres, err := conn.Query("Select Parameters from Parameters where JOBPARTSID = ?", part.PartID)
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

	return job, err
}

func (db *DB) SaveWork(work *Hyades.Work) error {

	CurrentClient, err := db.GetCurrentClientID(work.CurrentClient)
	if err != nil {
		return err
	}
	_, err = db.conn.Exec("UPDATE JOBPARTS set DispatchTime= ?, FinishTime = ?,	TotalTimeDispatched = ?,  CurrentClient = ?,	Done = ?,	Dispatched  = ?, BeingHandled  = ?,	FailCount = ?,	Error = ?,	Status = ?, Command = ? WHERE Id= ?;",
		work.DispatchTime,
		work.FinishTime,
		work.TotalTimeDispatched,
		CurrentClient,
		work.Done,
		work.Dispatched,
		work.BeingHandled,
		work.FailCount,
		work.Error,
		work.Status,
		work.Command,
		work.PartID)
	return err
}

func (db *DB) initDB() error {
	conn, err := sql.Open("sqlite3", "file:"+db.dbFile+"?_loc=auto")
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Exec("Create table if not exists JOBS  (Id INTEGER PRIMARY KEY AUTOINCREMENT,OwnerID INTEGER,Name varchar(255),JobFolder varchar(500),Env varchar(500),ReturnEnv TINYINT  );")

	if err != nil {
		return err
	}

	_, err = conn.Exec("Create table  if not exists JOBPARTS (Id INTEGER PRIMARY KEY AUTOINCREMENT,OwnerID INTEGER,DispatchTime datetime, FinishTime datetime,	TotalTimeDispatched INTEGER, CompletedBy   Integer, CurrentClient Integer,	Done TINYINT,	Dispatched  TINYINT, BeingHandled  TINYINT,	FailCount Integer,	Error varchar(500),	Status varchar(500), Command varchar(500));")

	if err != nil {
		return err
	}

	_, err = conn.Exec("Create table if not exists Parameters  (Id INTEGER PRIMARY KEY AUTOINCREMENT,JOBPARTSID INTEGER, Parameters varchar(500));")

	if err != nil {
		return err
	}

	_, err = conn.Exec("Create table if not exists CurrentClient  (Id INTEGER PRIMARY KEY AUTOINCREMENT,OperatingSystem varchar(100), ComputerName varchar(100));")

	if err != nil {
		return err
	}
	return nil

}

/*
OwnerID int32   `bson :"omitempty"`
	Id      int32   `json:"id" bson:"_id,omitempty"`
	Parts   []*Work `bson :"omitempty"`

	JobFolder string
	//A friendly name to used in displays
	Name string

	Env       []byte `bson :"omitempty"`
	ReturnEnv bool   `bson :"omitempty"`



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
*/
