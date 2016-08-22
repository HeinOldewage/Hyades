package Hyades

type Person struct {
	Id        int
	Username  string
	Password  []byte //SHA512 hash?
	Email     string
	Admin     bool
	Enabled   bool
	JobFolder string
}
