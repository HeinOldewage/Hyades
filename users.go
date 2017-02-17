package Hyades

type Person struct {
	Id       int64
	Username string
	//hash
	Password  []byte
	Email     string
	Admin     bool
	Enabled   bool
	JobFolder string
}
