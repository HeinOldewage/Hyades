package main

import (
	"fmt"

	"github.com/HeinOldewage/Hyades/server/databaseDefinition"

	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type DB struct {
	c databaseDefinition.DataBaseClient
}

func NewDB(server string) (*DB, error) {
	conn, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c := databaseDefinition.NewDataBaseClient(conn)
	db := &DB{c}
	db.init()
	return db, nil
}

func (db *DB) GetNextJob() (job *databaseDefinition.Job, work *databaseDefinition.Work, err error) {
	jw, err := db.c.GetNextJob(context.Background(), &google_protobuf.Empty{})
	if err == nil {
		fmt.Println("Got WorkID", jw.W, "/")
		return jw.GetJ(), jw.GetW(), nil
	} else {
		return nil, nil, err
	}
}

func (db *DB) GetCurrentClientID(c *databaseDefinition.ClientInfo) (int, error) {
	return 0, nil
}

func (db *DB) GetJob(id int64) (job *databaseDefinition.Job, err error) {
	return db.c.GetJob(context.Background(), &databaseDefinition.ID{id})
}

func (db *DB) GetPart(jobid, partid int64) (jobpart *databaseDefinition.Work, err error) {
	return db.c.GetPart(context.Background(), &databaseDefinition.JobWorkIdent{jobid, partid})
}

func (db *DB) SaveWork(work *databaseDefinition.Work) error {
	_, err := db.c.SaveWork(context.Background(), work)
	return err
}

func (db *DB) JobDone(id int64) error {
	_, err := db.c.JobDone(context.Background(), &databaseDefinition.ID{id})
	return err
}

func (db *DB) init() {

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
