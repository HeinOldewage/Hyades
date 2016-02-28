// +build windows
package main

import (
	"golang.org/x/sys/windows/svc"
	"log"
	"time"
)

func HandleService() chan int {
	res := make(chan int)
	go func() {
		isIntSess, err := svc.IsAnInteractiveSession()
		if err != nil {
			log.Fatalf("failed to determine if we are running in an interactive session: %v", err)
		}
		if !isIntSess {
			log.Println("runService(svcName, false)")
			svc.Run("Concordis", &myservice{res})
		} else {
			log.Println("A standard consle session")
		}
	}()
	return res
}

type myservice struct {
	response chan int
}

func (m *myservice) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (ssec bool, errno uint32) {
	const cmdsAccepted = svc.AcceptStop | svc.AcceptShutdown | svc.AcceptPauseAndContinue
	changes <- svc.Status{State: svc.StartPending}
	m.response <- start
	changes <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}
loop:
	for {
		select {
		case c := <-r:
			switch c.Cmd {
			case svc.Interrogate:
				changes <- c.CurrentStatus
				// Testing deadlock from https://code.google.com/p/winsvc/issues/detail?id=4
				time.Sleep(100 * time.Millisecond)
				changes <- c.CurrentStatus
			case svc.Stop, svc.Shutdown:
				m.response <- stop
				break loop
			case svc.Pause:
				changes <- svc.Status{State: svc.Paused, Accepts: cmdsAccepted}
				m.response <- pause
			case svc.Continue:
				changes <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}
				m.response <- resume
			default:
				log.Println("unexpected control request #%d", c)
			}
		}
	}
	changes <- svc.Status{State: svc.StopPending}
	close(m.response)
	return
}
