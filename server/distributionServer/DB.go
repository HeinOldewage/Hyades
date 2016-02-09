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
	Jobs []Job
}

type Job struct {
	Command string
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

func (db *DB) GetNextJob() *Job {
<<<<<<< HEAD
	query := []bson.M{{"$group": bson.M{"_id": nil, "count": bson.M{"$sum": "1"}}}}
	iterator := db.session.DB("Jobs").C("Users").Pipe(query).Iter()
=======
	query := []bson.M{{"$group": bson.M{"_id": bson.M{"PartOf.Owner": nil}, "count": bson.M{"$sum": 1}}}}
	iterator := db.session.DB("Jobs").C("Jobs").Pipe(query).Iter()
>>>>>>> 26efeafe3e9d17644a4c4816b1bc2652426098c8
	var res map[string]interface{} = make(map[string]interface{})

	for iterator.Next(res) {
		log.Println(res)
	}
<<<<<<< HEAD
	log.Println(iterator.Err())
	return nil
}

func init() {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		return
	}
	user := &User{Name: "Test"}
	user.Works = make([]Work, 1)
	user.Works[0].Jobs = make([]Job, 2)
	user.Works[0].Jobs[0].Command = "First command"
	user.Works[0].Jobs[0].Command = "Second command"

	session.DB("Jobs").C("Users").Insert(user)
}
=======
	return nil
}
>>>>>>> 26efeafe3e9d17644a4c4816b1bc2652426098c8
