package main

import (
	"github.com/HeinOldewage/Hyades"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	conn   *sql.DB
	dbFile string
}

func NewDB(DBFile string) (*DB, error) {

	conn, err := sql.Open("sqlite3", DBFile)
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
	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}

	res, err := tx.Query("Select * from JOBPARTS where BeingHandled = false and Dispatched = false and Done =false limit 1; ")
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	work = new(Hyades.Work)
	var CurrentClient int
	res.Scan(work.PartID,
		work.DispatchTime,
		work.FinishTime,
		work.CompletedBy,
		CurrentClient,
		work.Done,
		work.Dispatched,
		work.BeingHandled,
		work.FailCount,
		work.Error,
		work.Status,
		work.Command)

	res, err = tx.Query("UPDATE JOBPARTS where Id = % set (BeingHandled = true); ", work.PartID)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	tx.Commit()
	return work, nil
}
func (db *DB) GetCurrentClientID(c *Hyades.ClientInfo) (int, error) {
	res, err := db.conn.Query("Select Id from CurrentClient where OperatingSystem = % and ComputerName = % ; ", c.OperatingSystem, c.ComputerName)
	if err != nil {
		return 0, err
	}
	var id int64
	if res.Next() {
		res.Scan(&id)
	} else {
		res, err := db.conn.Exec("Insert into CurrentClient (OperatingSystem,ComputerName) values(% , %);", c.OperatingSystem, c.ComputerName)
		if err != nil {
			return 0, err
		}
		id, _ = res.LastInsertId()
	}
	return int(id), nil
}

func (db *DB) SaveWork(work *Hyades.Work) error {

	CurrentClient, err := db.GetCurrentClientID(work.CurrentClient)
	if err != nil {
		return err
	}
	_, err = db.conn.Exec("UPDATE JOBPARTS (DispatchTime=%, FinishTime = %,	TotalTimeDispatched = %, CompletedBy   = %, CurrentClient = %,	Done = %,	Dispatched  = %, BeingHandled  = %,	FailCount = %,	Error = %,	Status = %, Command = %) WHERE Id=%;",
		work.DispatchTime,
		work.FinishTime,
		work.CompletedBy,
		CurrentClient,
		work.Done,
		work.Dispatched,
		work.BeingHandled,
		work.FailCount,
		work.Error,
		work.Status,
		work.Command)
	return err
}

func (db *DB) initDB() error {
	conn, err := sql.Open("sqlite3", db.dbFile)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Exec("Create table if not exists JOBS  (Id INTEGER PRIMARY KEY AUTOINCREMENT,OwnerID INTEGER,Name varchar(255),Env varchar(255),ReturnEnv TINYINT  );")

	if err != nil {
		return err
	}

	_, err = conn.Exec("Create table  if not exists JOBPARTS (Id INTEGER PRIMARY KEY AUTOINCREMENT,OwnerID INTEGER,DispatchTime REAL, FinishTime REAL,	TotalTimeDispatched REAL, CompletedBy   Integer, CurrentClient Integer,	Done TINYINT,	Dispatched  TINYINT, BeingHandled  TINYINT,	FailCount Integer,	Error varchar(500),	Status varchar(500), Command varchar(500));")

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
