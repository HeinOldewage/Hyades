package Hyades

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Observable interface {
	AddObserver() *Observer
	GetChanges(uint32) ([]interface{}, bool)
}

type Observer struct {
	Id     uint32
	events chan interface{}
	ol     *ObserverList
}

type ObserverList struct {
	lock      *sync.Mutex
	observers map[uint32]*Observer
	nextid    uint32
}

func NewObserverList() *ObserverList {
	return &ObserverList{
		&sync.Mutex{},
		make(map[uint32]*Observer, 0),
		0,
	}
}

func (ol *ObserverList) AddObserver() *Observer {
	ol.lock.Lock()
	defer ol.lock.Unlock()
	res := &Observer{
		atomic.AddUint32(&ol.nextid, 1),
		make(chan interface{}, 100),
		ol,
	}

	ol.observers[res.Id] = res

	return res
}

func (ol *ObserverList) GetChanges(id uint32) ([]interface{}, bool) {
	ob, ok := ol.GetObserver(id)
	if !ok {
		return nil, false
	}
	return ob.GetEvents(), true
}

func (ol *ObserverList) GetObserver(id uint32) (ob *Observer, ok bool) {
	ol.lock.Lock()
	defer ol.lock.Unlock()
	ob, ok = ol.observers[id]
	return
}

func (ol *ObserverList) RemoveObserver(ob *Observer) {
	ol.lock.Lock()
	defer ol.lock.Unlock()

	delete(ol.observers, ob.Id)

}

func (ol *ObserverList) Callback(e interface{}) {
	ol.lock.Lock()
	defer ol.lock.Unlock()
	for _, ob := range ol.observers {
		ob.Callback(e)
	}
}

func (wo *Observer) Callback(e interface{}) {
	select {
	case wo.events <- e:
		{
			//log.Println("observer", wo.Id, " got an event")
		}
	default:
		{
			//If the observer fails to retrive events we delete it
			log.Println("Removing observer", wo.Id)
			go wo.ol.RemoveObserver(wo)

		}

	}
}

func (wo *Observer) GetEvents() []interface{} {
	res := make([]interface{}, 0)
	timer := time.After(time.Second * 30)
	//Wait for at least 1 event or 30 seconds to pass
	select {
	case event := <-wo.events:
		{
			res = append(res, event)
		}
	case <-timer:
		{

		}
	}
	//Get the rest of the events, if any exist
	for {
		select {
		case event := <-wo.events:
			{
				res = append(res, event)
			}
		default:
			{
				return res
			}
		}
	}
}
