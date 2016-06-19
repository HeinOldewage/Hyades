package main

import (
	"log"
	"strconv"
	"time"

	"github.com/HeinOldewage/Hyades"

	"sync"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type DB struct {
	session *mgo.Session
	lock    sync.Mutex
}

func NewDB(username, pasword string) (*DB, error) {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		return nil, err
	}
	err = session.DB("Hyades").Login(username, pasword)
	if err != nil {
		log.Println("Could not login")
		return nil, err
	}
	return &DB{session: session}, nil
}

func (db *DB) GetNextJob() *Hyades.Work {
	db.lock.Lock()
	defer db.lock.Unlock()
	for {
		query := []bson.M{{"$unwind": bson.M{"path": "$parts", "includeArrayIndex": "index"}}, {"$project": bson.M{"parts": 1, "_id": 1, "index": 1}}, {"$match": bson.M{"parts.beinghandled": false}}, {"$match": bson.M{"parts.done": false}}, {"$match": bson.M{"parts.dispatched": false}}}
		iterator := db.session.DB("Hyades").C("Jobs").Pipe(query).Iter()

		var res map[string]interface{} = make(map[string]interface{})
		//var res Hyades.WorkComms
		for iterator.Next(&res) {

			//Set to being handled
			updater := bson.M{"$set": bson.M{"parts." + strconv.FormatInt(res["index"].(int64), 10) + ".beinghandled": true}}
			err := db.session.DB("Hyades").C("Jobs").UpdateId(res["_id"].(bson.ObjectId), updater)
			if err != nil {
				log.Println("GetNextJob Update", err)
				continue
			}
			var job Hyades.Job
			err = db.session.DB("Hyades").C("Jobs").FindId(res["_id"].(bson.ObjectId)).One(&job)
			if err != nil {
				log.Println("GetNextJob select", err)
				continue
			}
			log.Println("Returning job with ID", res["_id"].(bson.ObjectId), job.Id)
			job.Setup()
			return job.Parts[res["index"].(int64)]
		}
		if iterator.Err() != nil {
			panic(iterator.Err())
		}
		time.Sleep(time.Second * 1)
	}

	return nil
}

func (db *DB) SaveWork(work *Hyades.Work) error {
	return work.Save(db.session)
}

func (db *DB) ResetBeingDone() error {
	f := func() error {
		return db.session.DB("Hyades").C("Jobs").Update(bson.M{"parts.beinghandled": true}, bson.M{"$set": bson.M{"parts.$.beinghandled": false}})
	}

	for f() == nil {
	}

	log.Println("ResetBeingDone", f())

	return nil
}

func init() {

	return
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		return
	}

	session.DB("Hyades").DropDatabase()
	session.DB("Hyades").C("Jobs").DropCollection()
	session.DB("Hyades").C("Users").DropCollection()

	dbnames, _ := session.DatabaseNames()
	for _, name := range dbnames {
		if name == "Admin" {
			log.Println("Skipping DB init")
			return
		}
	}
	session.DB("Hyades").DropDatabase()
	session.DB("Hyades").C("Jobs").DropCollection()
	session.DB("Hyades").C("Users").DropCollection()

	user := &Hyades.Person{Username: "Test"}

	session.DB("Hyades").C("Users").Insert(user)
	err = session.DB("Hyades").C("Users").Find(user).One(user)
	if err != nil {
		log.Println(err)
	}

	toInsertJob := &Hyades.Job{}

	toInsertJob.OwnerID = bson.ObjectId(user.Id)
	Hyades.NewWork(toInsertJob, "1", "echo 'test'")
	Hyades.NewWork(toInsertJob, "2", "echo 'hello'")
	Hyades.NewWork(toInsertJob, "3", "echo 'world'").Done = true

	err = session.DB("Hyades").C("Jobs").Insert(toInsertJob)
	if err != nil {
		log.Println("Insert Error:", err)
	}

	iterator := session.DB("Hyades").C("Jobs").Find(nil).Iter()

	var res map[string]interface{} = make(map[string]interface{})
	//var res User

	for iterator.Next(&res) {
		log.Println(res)
	}
	log.Println("Err:", iterator.Err())

}
