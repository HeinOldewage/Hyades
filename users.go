package Hyades

type Person struct {
	Id       int
	Username string
	//hash
	Password  []byte
	Email     string
	Admin     bool
	Enabled   bool
	JobFolder string
}
