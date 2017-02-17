package main

import (
	"io"
	"log"

	"github.com/HeinOldewage/Hyades"

	"github.com/HeinOldewage/Hyades/server/databaseDefinition"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type JobMap struct {
	c databaseDefinition.DataBaseClient
}

func NewJobMap(server string) (*JobMap, error) {
	conn, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	c := databaseDefinition.NewDataBaseClient(conn)
	return &JobMap{c}, nil
}

func (jm *JobMap) NewJob(user *Hyades.Person) *Hyades.Job {
	return &Hyades.Job{OwnerID: user.Id}
}

func (jm *JobMap) GetJob(id int64) (*Hyades.Job, error) {
	dbjob, err := jm.c.GetJob(context.Background(), &databaseDefinition.ID{id})
	if err != nil {
		return nil, err
	}
	resjob := &Hyades.Job{}
	databaseDefinition.LoadInto(resjob, dbjob)

	stream, err := jm.c.GetWorks(context.Background(), &databaseDefinition.ID{resjob.Id})
	if err != nil {
		return nil, err
	}
	for {
		w, err := stream.Recv()
		if err == io.EOF {
			break
		} else {
			if err != nil {
				return resjob, err
			}
		}
		aw := &Hyades.Work{}
		databaseDefinition.LoadInto(aw, w)
		resjob.Parts = append(resjob.Parts, aw)
	}
	log.Println("Job", resjob.Id, "has", len(resjob.Parts), "parts")

	return resjob, nil
}

func (jm *JobMap) GetAll(user *Hyades.Person) (jobs []*Hyades.Job, err error) {
	jobs = make([]*Hyades.Job, 0)
	s, err := jm.c.GetAll(context.Background(), &databaseDefinition.ID{user.Id})

	if err != nil {
		return nil, err
	}

	for {
		j, err := s.Recv()
		if err == io.EOF {
			break
		} else {
			if err != nil {
				return jobs, err
			}
		}
		aj := &Hyades.Job{}
		databaseDefinition.LoadInto(aj, j)
		jobs = append(jobs, aj)
	}
	for _, job := range jobs {
		stream, err := jm.c.GetWorks(context.Background(), &databaseDefinition.ID{job.Id})
		if err != nil {
			return nil, err
		}
		for {
			w, err := stream.Recv()
			if err == io.EOF {
				break
			} else {
				if err != nil {
					return jobs, err
				}
			}
			aw := &Hyades.Work{}
			databaseDefinition.LoadInto(aw, w)
			job.Parts = append(job.Parts, aw)
		}
		log.Println("Job", job.Id, "has", len(job.Parts), "parts")
	}

	return jobs, nil
}

func (jm *JobMap) GetAllWithoutWork(user *Hyades.Person) (jobs []*Hyades.Job, err error) {
	jobs = make([]*Hyades.Job, 0)
	s, err := jm.c.GetAll(context.Background(), &databaseDefinition.ID{user.Id})

	if err != nil {
		return nil, err
	}

	for {
		j, err := s.Recv()
		if err == io.EOF {
			break
		} else {
			if err != nil {
				return jobs, err
			}
		}
		aj := &Hyades.Job{}
		databaseDefinition.LoadInto(aj, j)
		jobs = append(jobs, aj)
	}

	return jobs, nil
}

func (jm *JobMap) AddJob(job *databaseDefinition.Job, work []*databaseDefinition.Work) error {

	ID, err := jm.c.AddJob(context.Background(), job)
	if err != nil {
		log.Println("AddJob", err)
	} else {
		job.Id = ID.GetID()
		log.Println("AddJob returned with no error")
	}

	for k := range work {
		work[k].PartOfID = job.Id
		work[k].PartID = int64(k)
	}

	stream, err := jm.c.AddWorks(context.Background())
	if err != nil {
		log.Println("AddJob", err)
		return err
	}
	for w := range work {
		log.Println("work[w].PartOfID", work[w].PartOfID)
		err := stream.Send(work[w])
		if err != nil {
			log.Println("AddJob", err)
		}
	}
	return err
}

func (jm *JobMap) Delete(job *Hyades.Job) error {
	_, err := jm.c.DeleteJob(context.Background(), &databaseDefinition.ID{job.Id})
	return err
}
