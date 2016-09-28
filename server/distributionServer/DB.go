package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/HeinOldewage/Hyades"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	sync.Mutex
	dbFile string
	conn   *sql.DB
}

func NewDB(DBFile string) (*DB, error) {

	conn, err := sql.Open("sqlite3", "file:"+DBFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return nil, err
	}

	res := &DB{dbFile: DBFile, conn: conn}
	err = res.initDB()

	res.GetCurrentClientID(&Hyades.ClientInfo{"H", "W"})
	res.GetCurrentClientID(&Hyades.ClientInfo{"H", "W"})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *DB) GetNextJob() (work *Hyades.Work, err error) {
	db.Lock()
	defer db.Unlock()
	log.Println("(db *DB) GetNextJob() start")
	//transaction this
	var sleepCounter int = 0
	var jobId int
	for {
		var err error
		var ok bool
		log.Println("Trying to get a job")

		work, jobId, err, ok = db.tryGetJob()
		log.Println("tryGetJob returned")
		if err != nil {
			log.Println("(db *DB) GetNextJob()", err)
		} else if ok {
			break
		} else {
			if sleepCounter < 60 {
				sleepCounter++
			}
			time.Sleep(time.Second * time.Duration(sleepCounter))
		}

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

func (db *DB) tryGetJob() (*Hyades.Work, int, error, bool) {

	conn, err := sql.Open("sqlite3", "file:"+db.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return nil, 0, err, false
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Println("Error closing conn")
		} else {
			log.Println("Closed db conn")
		}
	}()
	tx, err := conn.Begin()
	if err != nil {
		return nil, -1, err, false
	}
	defer tx.Rollback()

	var jobId int

	res, err := tx.Query("Select OwnerID,Id,DispatchTime,FinishTime,TotalTimeDispatched,Done,Dispatched,BeingHandled,FailCount,Error,Status,Command from JOBPARTS where ((BeingHandled = ? ) and (Dispatched = ? ) and (Done = ? )) limit 1; ", false, false, false)
	if err != nil {
		log.Println("Could not select a job", err)
		return nil, -1, err, false
	}
	defer closeQuery(res)
	work := new(Hyades.Work)

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
			return nil, -1, err, false
		}

		if work.BeingHandled {
			log.Println("Work is already being handled")
		}
	} else {

		return nil, -1, nil, false
	}

	ures, err := tx.Exec("UPDATE JOBPARTS  set BeingHandled = ? where Id = ?; ", true, work.PartID)
	if err != nil {
		log.Println("Could not update a job", err)
		return nil, -1, err, false
	}

	i, _ := ures.RowsAffected()
	if i != 1 {
		log.Println("Updating did not affect one row", i)
	}
	err = tx.Commit()
	if err != nil {
		log.Println("Could not commit job chekout", err)
		return nil, 0, err, false
	}
	return work, jobId, nil, true

}

func (db *DB) GetCurrentClientID(c *Hyades.ClientInfo) (int, error) {
	log.Println("GetCurrentClientID started")
	defer log.Println("GetCurrentClientID done")

	conn, err := sql.Open("sqlite3", "file:"+db.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if c == nil {
		return 0, nil
	}
	res, err := conn.Query("Select Id from CurrentClient where OperatingSystem = ? and ComputerName = ? ; ", c.OperatingSystem, c.ComputerName)
	if err != nil {
		return 0, err
	}
	var id int64
	defer closeQuery(res)
	if res.Next() {
		res.Scan(&id)
		res.Close()
	} else {
		res.Close()
		res, err := conn.Exec("Insert into CurrentClient (OperatingSystem,ComputerName) values(? , ?);", c.OperatingSystem, c.ComputerName)
		if err != nil {
			return 0, err
		}
		id, _ = res.LastInsertId()
	}
	return int(id), nil
}

func (db *DB) GetJob(id int) (job *Hyades.Job, err error) {
	conn, err := sql.Open("sqlite3", "file:"+db.dbFile+"?_loc=auto&_busy_timeout=60000")
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

	return job, err
}

func (db *DB) SaveWork(work *Hyades.Work) error {

	db.Lock()
	defer db.Unlock()

	log.Println("SaveWork started")
	defer log.Println("SaveWork done")

	CurrentClient, err := db.GetCurrentClientID(work.CurrentClient)
	if err != nil {
		log.Println("db *DB) SaveWork db.GetCurrentClientID error", err)
		return err
	}

	conn, err := sql.Open("sqlite3", "file:"+db.dbFile+"?_loc=auto&_busy_timeout=60000")

	defer conn.Close()
	if err != nil {
		log.Println("db *DB) SaveWork sql.Open error", err)
		return err
	}

	tx, err := conn.Begin()
	defer tx.Commit()
	_, err = tx.Exec("UPDATE JOBPARTS set DispatchTime= ?, FinishTime = ?, TotalTimeDispatched = ?,  CurrentClient = ?, Done = ?, Dispatched  = ?, BeingHandled  = ?, FailCount = ?, Error = ?, Status = ?, Command = ? WHERE Id= ?;",
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

	if err != nil {
		log.Println("db *DB) SaveWork tx.Exec error", err)
	}

	return err
}

func (db *DB) initDB() error {
	conn, err := sql.Open("sqlite3", "file:"+db.dbFile+"?_loc=auto&_busy_timeout=60000")
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

	_, err = conn.Exec("Create INDEX if not exists DoneIndex On  JOBPARTS (Done);")

	if err != nil {
		return err
	}

	_, err = conn.Exec("Create INDEX if not exists BeingHandledIndex On  JOBPARTS (BeingHandled);")

	if err != nil {
		return err
	}

	_, err = conn.Exec("Create INDEX if not exists DispatchedIndex On  JOBPARTS (Dispatched);")

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

	_, err = conn.Exec("UPDATE JOBPARTS  set BeingHandled = ? ; ", false)
	if err != nil {
		return err
	}
	return nil

}
func closeQuery(conn *sql.Rows) {
	conn.Close()
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
