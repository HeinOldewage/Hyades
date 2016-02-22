package Hyades

type Jobqqqq struct {
	Command         string
	Env             []byte
	SaveEnvironment bool
}

type JobResult struct {
	Env         []byte
	StdOut      []byte
	StdErr      []byte
	SystemError string
}
