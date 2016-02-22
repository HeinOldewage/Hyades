package main

import (
	"github.com/HeinOldewage/Hyades"
	"bytes"
	"crypto/sha512"
	"encoding/gob"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

type UserMap struct {
	UserID   int32
	Users    map[string]*Hyades.Person
	userLock *sync.Mutex
}

func NewUserMap(filename string) (*UserMap, error) {
	var userMap UserMap

	file, err := os.Open(filename)
	if err != nil {
		return emptyUserMap(), err
	} else {
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(&userMap)
		if err != nil {
			return emptyUserMap(), err
		}
		file.Close()
		userMap.userLock = &sync.Mutex{}
		return &userMap, nil
	}
}

func emptyUserMap() *UserMap {
	return &UserMap{
		0,
		make(map[string]*Hyades.Person),
		&sync.Mutex{},
	}
}

func (um *UserMap) Save(filename string) error {
	um.userLock.Lock()
	defer um.userLock.Unlock()

	usersFile, err := os.Create(filename)
	if err != nil {
		return err
	}

	encoder := gob.NewEncoder(usersFile)
	encoder.Encode(*um)

	usersFile.Close()
	return nil
}

func (um *UserMap) addUser(u, p string) (*Hyades.Person, bool) {
	um.userLock.Lock()
	defer um.userLock.Unlock()

	if _, ok := um.Users[u]; ok {
		//User exists, do not create another one
		return nil, false
	}

	hh := sha512.Sum512([]byte(p))
	h := make([]byte, 64)
	for k := 0; k < 64; k++ {
		h[k] = hh[k]
	}
	admin := len(um.Users) == 0 //First user is made admin
	person := &Hyades.Person{
		u,
		h,
		"",
		admin,
		admin, //Only enabled by default if the person is admin
		fmt.Sprint(atomic.AddInt32(&um.UserID, 1)),
	}
	um.Users[u] = person
	return person, true
}

func (um *UserMap) findUser(u, p string) (*Hyades.Person, bool) {
	um.userLock.Lock()
	defer um.userLock.Unlock()
	person, ok := um.Users[u]
	if !ok {
		return nil, false
	}
	hh := sha512.Sum512([]byte(p))
	h := make([]byte, 64)
	for k := 0; k < 64; k++ {
		h[k] = hh[k]
	}
	if bytes.Compare(person.Password, h) == 0 {
		return person, true
	} else {
		return nil, false
	}
}
