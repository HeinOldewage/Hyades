package main

import (
	"log"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type User struct {
	Name     string
	Password []byte
	Works    []Work
}

type Work struct {
	//Environment file saved on the server
	Env  string
	Jobs []Job
}

type Job struct {
	Command         string
	SaveEnvironment bool
	Done            bool
}

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

func (db *DB) GetNextJob() *RunnableJob {

	//query := []bson.M{{"$unwind": "$works"}, {"$unwind": "$works.jobs"}, {"$group": bson.M{"_id": bson.M{"name": "$name"}, "sumation": bson.M{"$sum": 1}}}}
	query := []bson.M{{"$unwind": "$works"}, {"$unwind": "$works.jobs"}, {"$match": bson.M{"works.jobs.done": bson.M{"$eq": false}}}}
	iterator := db.session.DB("Jobs").C("Jobs").Pipe(query).Iter()

	//var res map[string]interface{} = make(map[string]interface{})
	var res RunnableJob
	for iterator.Next(&res) {

		log.Println(res)
		return &res
	}
	log.Println("Err:", iterator.Err())
	return nil
}

type RunnableJob struct {
	Name     string
	Password []byte

	Works struct {
		Env  string
		Jobs Job
	}
}

func init() {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		return
	}
	session.DB("Jobs").C("Jobs").DropCollection()

	user := &User{Name: "Test"}
	user.Works = make([]Work, 1)
	user.Works[0].Jobs = make([]Job, 2)
	user.Works[0].Jobs[0].Command = "First command"
	user.Works[0].Jobs[0].Done = true
	user.Works[0].Jobs[1].Command = "Second command"
	user.Works[0].Jobs[1].Done = false

	log.Println("Insert Error:", session.DB("Jobs").C("Jobs").Insert(user))

	iterator := session.DB("Jobs").C("Jobs").Find(nil).Iter()

	var res map[string]interface{} = make(map[string]interface{})
	//var res User

	for iterator.Next(&res) {
		log.Println(res)
	}
	log.Println("Err:", iterator.Err())
}
