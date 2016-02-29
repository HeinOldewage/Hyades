package main

import (
	"log"
	"time"

	"github.com/HeinOldewage/Hyades"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type DB struct {
	session *mgo.Session
}

func NewDB() (*DB, error) {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		return nil, err
	}
	return &DB{session}, nil
}

func (db *DB) GetNextJob() *Hyades.Work {

	for {
		//query := []bson.M{{"$unwind": "$parts"}, {"$match": bson.M{"parts.done": false}}, {"$match": bson.M{"parts.dispatched": false}}}
		query := []bson.M{{"$unwind": bson.M{"path": "$parts", "includeArrayIndex": "index"}}, {"$match": bson.M{"parts.done": false}}, {"$match": bson.M{"parts.dispatched": false}}}
		iterator := db.session.DB("Admin").C("Jobs").Pipe(query).Iter()

		var res map[string]interface{} = make(map[string]interface{})
		//var res Hyades.WorkComms
		for iterator.Next(&res) {
			var job Hyades.Job
			err := db.session.DB("Admin").C("Jobs").FindId(res["_id"].(bson.ObjectId)).One(&job)
			if err != nil {
				log.Println("GetNextJob", err)
				continue
			}
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

func init() {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		return
	}
	session.DB("Admin").DropDatabase()
	session.DB("Admin").C("Jobs").DropCollection()
	session.DB("Admin").C("Users").DropCollection()
	return
	dbnames, _ := session.DatabaseNames()
	for _, name := range dbnames {
		if name == "Admin" {
			log.Println("Skipping DB init")
			return
		}
	}
	session.DB("Admin").DropDatabase()
	session.DB("Admin").C("Jobs").DropCollection()
	session.DB("Admin").C("Users").DropCollection()

	user := &Hyades.Person{Username: "Test"}

	session.DB("Admin").C("Users").Insert(user)
	err = session.DB("Admin").C("Users").Find(user).One(user)
	if err != nil {
		log.Println(err)
	}

	toInsertJob := &Hyades.Job{}

	toInsertJob.OwnerID = user.Id
	Hyades.NewWork(toInsertJob, "1", "echo 'test'")
	Hyades.NewWork(toInsertJob, "2", "echo 'hello'")
	Hyades.NewWork(toInsertJob, "3", "echo 'world'").Done = true

	err = session.DB("Admin").C("Jobs").Insert(toInsertJob)
	if err != nil {
		log.Println("Insert Error:", err)
	}

	iterator := session.DB("Admin").C("Jobs").Find(nil).Iter()

	var res map[string]interface{} = make(map[string]interface{})
	//var res User

	for iterator.Next(&res) {
		log.Println(res)
	}
	log.Println("Err:", iterator.Err())

}
