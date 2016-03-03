package Hyades

import "gopkg.in/mgo.v2/bson"

type Person struct {
	Id        bson.ObjectId `json:"id" bson:"_id,omitempty"`
	Username  string
	Password  []byte //SHA512 hash?
	Email     string
	Admin     bool
	Enabled   bool
	JobFolder string
}
