package main

import (
	"log"

	"github.com/HeinOldewage/Hyades"

	"golang.org/x/crypto/bcrypt"

	"encoding/gob"
	"reflect"

	"github.com/boltdb/bolt"

	"bytes"
	"encoding/binary"
	"errors"
)

type UserMap struct {
	db *bolt.DB
}

func NewUserMap(dbFile string) *UserMap {
	db, err := bolt.Open("users.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &UserMap{db}
}

func (um *UserMap) addUser(u, p string) (*Hyades.Person, bool) {
	log.Println("Add User", u, p)
	Password, _ := bcrypt.GenerateFromPassword([]byte(p), bcrypt.DefaultCost)

	person := Hyades.Person{
		Username:  u,
		Password:  Password,
		Enabled:   true,
		JobFolder: u}

	err := um.db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return err
		}
		i, err := b.NextSequence()
		if err != nil {
			return err
		}
		person.Id = int64(i)
		return SaveToBucket(b, []byte(u), person)
	})

	if err != nil {
		panic(err)
	}

	return um.findUser(u, p)
}

func (um *UserMap) getUser(u string) *Hyades.Person {
	user := new(Hyades.Person)

	um.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("users"))

		return LoadFromBucket(b, []byte(u), user)
	})

	return user

}

func (um *UserMap) findUser(u, p string) (*Hyades.Person, bool) {

	user := new(Hyades.Person)

	err := um.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("users"))
		if b == nil {
			return errors.New("Bucket does not yet exist")
		}

		return LoadFromBucket(b, []byte(u), user)
	})

	if err != nil {
		return nil, false
	}

	if bcrypt.CompareHashAndPassword(user.Password, []byte(p)) == nil {

		return user, true
	} else {

		return user, false
	}
}

func SaveToBucket(b *bolt.Bucket, key []byte, value interface{}) error {
	val := reflect.ValueOf(value)
	typ := reflect.TypeOf(value)

	if typ.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
		typ = val.Type()
	}

	if typ.Kind() == reflect.Struct {
		buc, err := b.CreateBucketIfNotExists(key)
		if err != nil {
			return err
		}
		for k := 0; k < val.NumField(); k++ {
			err := SaveToBucket(buc, []byte(typ.Field(k).Name), val.Field(k).Interface())
			if err != nil {
				return err
			}
		}
	} else {
		val := &bytes.Buffer{}
		err := gob.NewEncoder(val).Encode(value)
		if err != nil {
			return err
		}
		err = b.Put(key, val.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

func LoadFromBucket(b *bolt.Bucket, key []byte, value interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(value))
	typ := val.Type()

	if typ.Kind() == reflect.Struct {
		buc := b.Bucket(key)
		if buc == nil {
			return errors.New("key not in bucket")
		}
		for k := 0; k < val.NumField(); k++ {
			err := LoadFromBucket(buc, []byte(typ.Field(k).Name), val.Field(k).Addr().Interface())
			if err != nil {
				return err
			}
		}
	} else {
		val := bytes.NewBuffer(b.Get(key))
		err := gob.NewDecoder(val).Decode(value)
		if err != nil {
			return err
		}

	}
	return nil
}

// itob returns an 8-byte big endian representation of v.
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btoi(v []byte) (k uint64) {

	k = binary.BigEndian.Uint64(v)
	return
}
