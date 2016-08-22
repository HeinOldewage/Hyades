package main

import (
	"log"

	"github.com/HeinOldewage/Hyades"

	"golang.org/x/crypto/bcrypt"
)

type UserMap struct {
	dbFile string
}

func NewUserMap(dbFile string) *UserMap {
	return &UserMap{dbFile}
}

func (um *UserMap) addUser(u, p string) (*Hyades.Person, bool) {
	log.Println("Add User", u, p)
	Password, _ := bcrypt.GenerateFromPassword([]byte(p), bcrypt.DefaultCost)

	find := make(map[string]interface{})
	find["username"] = u

	n, err := um.session.DB("Hyades").C("Users").Find(find).Count()
	if err != nil || n != 0 {
		log.Println("Error, Already exists", err, n)
		return nil, false
	}
	user := Hyades.Person{Username: u, Password: Password}
	err = um.session.DB("Hyades").C("Users").Insert(user)
	log.Println("Inserted result", err, user)
	if err != nil {
		return nil, false
	}
	return um.findUser(u, p)
}

func (um *UserMap) findUser(u, p string) (*Hyades.Person, bool) {
	log.Println("find User", u, p)

	user := Hyades.Person{}
	find := make(map[string]interface{})
	find["username"] = u

	err := um.session.DB("Hyades").C("Users").Find(find).One(&user)
	if err != nil {
		log.Println("Cannot find ", find, " due to:", err)
		return nil, false
	}

	if bcrypt.CompareHashAndPassword(user.Password, []byte(p)) == nil {

		return &user, true
	} else {

		return &user, false
	}
}
