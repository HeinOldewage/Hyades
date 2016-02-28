package Hyades

type Person struct {
	Id        string `json:"id" bson:"_id,omitempty"`
	Username  string
	Password  []byte //SHA512 hash?
	Email     string
	Admin     bool
	Enabled   bool
	JobFolder string
}
