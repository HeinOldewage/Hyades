package main

import (
	"log"

	"github.com/HeinOldewage/Hyades"

	"golang.org/x/crypto/bcrypt"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type UserMap struct {
	dbFile string
}

func NewUserMap(dbFile string) *UserMap {
	res := &UserMap{dbFile}
	res.initDB()
	return res
}

func (um *UserMap) addUser(u, p string) (*Hyades.Person, bool) {
	log.Println("Add User", u, p)
	Password, _ := bcrypt.GenerateFromPassword([]byte(p), bcrypt.DefaultCost)

	conn, err := sql.Open("sqlite3", "file:"+um.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		log.Println(err)
		return nil, false
	}
	defer conn.Close()
	_, err = conn.Exec("insert into USERS (Username,Password) values ( ? , ? ) ;", u, Password)

	if err != nil {
		log.Println(err)
		return nil, false
	}
	return um.findUser(u, p)
}

func (um *UserMap) initDB() error {
	conn, err := sql.Open("sqlite3", "file:"+um.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Exec("Create table if not exists USERS  (Id INTEGER PRIMARY KEY AUTOINCREMENT,Username varchar(255),Password blob,Email varchar(255),Admin TINYINT ,Enabled TINYINT,JobFolder varchar(255) );")

	if err != nil {
		return err
	}

	return nil

}

func (um *UserMap) getUser(u string) *Hyades.Person {

	conn, err := sql.Open("sqlite3", "file:"+um.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query("select * from USERS where Username = ?", u)

	if err != nil {
		return nil
	}
	user := new(Hyades.Person)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&user.Id, &user.Username, &user.Password, &user.Email, &user.Admin, &user.Enabled, &user.JobFolder)
		if err != nil {
			log.Println(err)
		}
	}

	return user

}

func (um *UserMap) findUser(u, p string) (*Hyades.Person, bool) {
	log.Println("find User", u, p)

	conn, err := sql.Open("sqlite3", "file:"+um.dbFile+"?_loc=auto&_busy_timeout=60000")
	if err != nil {
		return nil, false
	}
	defer conn.Close()
	rows, err := conn.Query("select * from USERS where Username = ?", u)

	if err != nil {
		return nil, false
	}
	user := new(Hyades.Person)
	defer closeQuery(rows)
	for rows.Next() {
		//Id ,Username ,Password ,Email Admin  ,Enabled ,JobFolder
		err = rows.Scan(&user.Id, &user.Username, &user.Password, &user.Email, &user.Admin, &user.Enabled, &user.JobFolder)
		if err != nil {
			log.Println(err)
		}

		log.Println("Found user", user)
	}
	if bcrypt.CompareHashAndPassword(user.Password, []byte(p)) == nil {

		return user, true
	} else {

		return user, false
	}
}
