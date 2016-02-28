package main

import (
	"log"

	"github.com/HeinOldewage/Hyades"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/mgo.v2"
	//	"gopkg.in/mgo.v2/bson"
)

type UserMap struct {
	session *mgo.Session
}

func NewUserMap(session *mgo.Session) *UserMap {
	return &UserMap{session}
}

func (um *UserMap) addUser(u, p string) (*Hyades.Person, bool) {
	Password, _ := bcrypt.GenerateFromPassword([]byte(p), bcrypt.DefaultCost)
	user := Hyades.Person{Username: u, Password: Password}
	n, err := um.session.DB("Admin").C("Users").Find(user).Count()
	if err != nil || n != 0 {
		return nil, false
	}
	err = um.session.DB("Admin").C("Users").Insert(user)
	if err != nil {
		return nil, false
	}
	return &user, true
}

func (um *UserMap) findUser(u, p string) (*Hyades.Person, bool) {
	Password, _ := bcrypt.GenerateFromPassword([]byte(p), bcrypt.DefaultCost)
	user := Hyades.Person{Username: u, Password: Password}
	log.Println(um, um.session)
	err := um.session.DB("Admin").C("Users").Find(user).One(&user)
	if err != nil {
		return nil, false
	}
	return &user, true
}
