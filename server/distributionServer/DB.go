package main

import (
	"log"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type User struct {
}

type Work struct {
	Owner User
}

type Job struct {
	PartOf Work
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
	query := []bson.M{{"$group": bson.M{"_id": bson.M{"PartOf.Owner": nil}, "count": bson.M{"$sum": 1}}}}
	iterator := db.session.DB("Jobs").C("Jobs").Pipe(query).Iter()
	var res map[string]interface{} = make(map[string]interface{})

	for iterator.Next(res) {
		log.Println(res)
	}
	return nil
}
