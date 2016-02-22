package Hyades

type Person struct {
	Username  string
	Password  []byte //SHA512 hash?
	Email     string
	Admin     bool
	Enabled   bool
	JobFolder string
}
