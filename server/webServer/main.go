package main

import (
	"fmt"
	"net/http"
)

func main() {
	fmt.Println("This is the web server")
	http.Handle("/", http.FileServer(http.Dir("./resources")))
	err := http.ListenAndServe(":80", nil)
	if err != nil {
		panic(err)
	}

}
