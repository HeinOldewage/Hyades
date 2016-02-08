package main

import (
	"fmt"
	"log"
	"net"
)

type TaurusClient struct {
	ServerAddress string
	conn          net.Conn
}

func (tc *TaurusClient) Connect() (err error) {
	tc.conn, err = net.Dial("tcp", tc.ServerAddress)
	return err
}

func main() {
	fmt.Println("This is the taurus client")
	tc := TaurusClient{ServerAddress: ":8080"}
	err := tc.Connect()
	if err != nil {
		log.Println(err)
	}
	select {}

}
