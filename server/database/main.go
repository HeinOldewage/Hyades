package main

import (
	"errors"
	"flag"
	"io"
	"time"

	"github.com/HeinOldewage/Hyades/server/databaseDefinition"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	context "golang.org/x/net/context"

	"google.golang.org/grpc"

	"net"

	"fmt"

	"encoding/binary"
	"log"

	"github.com/boltdb/bolt"

	"bytes"
	"encoding/gob"
	"reflect"
)

var port = flag.Int("port", 8085, "Server Port")

func main() {

	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	databaseDefinition.RegisterDataBaseServer(grpcServer, newServer())

	grpcServer.Serve(lis)

}

type server struct {
	db *bolt.DB
}

func newServer() *server {
	db, err := bolt.Open("jobs.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &server{db: db}
}

func (s *server) GetNextJob(context.Context, *google_protobuf.Empty) (*databaseDefinition.JobWork, error) {
	res := &databaseDefinition.JobWork{J: new(databaseDefinition.Job)}
	found := false
	var err error
	for !found {

		err = s.db.Batch(func(tx *bolt.Tx) error {
			jsb := tx.Bucket([]byte("jobs"))
			if jsb == nil {
				return errors.New("No jobs")
			}

			return jsb.ForEach(func(jobKey, v []byte) error {
				if found {
					return nil
				}

				jb := jsb.Bucket(jobKey)

				wb := jb.Bucket([]byte("Parts"))

				if wb == nil {
					return nil
				}
				wb.ForEach(func(partKey, v []byte) error {
					if found {
						return nil
					}

					work := &databaseDefinition.Work{}
					err := LoadFromBucket(wb, partKey, work)
					if err != nil {
						return err
					}
					if !work.Dispatched && !work.Done {
						found = true
						work.Dispatched = true
						err := SaveToBucket(wb, partKey, work)
						if err != nil {
							fmt.Println("Failed to save dispatch status", err)
							return err
						}
						res.W = work
						err = LoadFromBucket(jsb, jobKey, res.J)
						if err != nil {
							fmt.Println("Failed to load job")
							return err
						}
					}

					return nil
				})

				return nil
			})

		})

		if err != nil {
			break
		}
		if !found {
			time.Sleep(time.Second)
		}
	}
	return res, err
}

/*
func (s *server) GetCurrentClientID(context.Context, *databaseDefinition.ClientInfo) (*databaseDefinition.ID, error) {

}*/

func (s *server) AddWorks(stream databaseDefinition.DataBase_AddWorksServer) error {
	errchan := make(chan error)
	for {
		select {
		case err := <-errchan:
			return err

		default:

		}
		work, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		go func(work *databaseDefinition.Work) {
			err := s.db.Batch(func(tx *bolt.Tx) error {
				jsb := tx.Bucket([]byte("jobs"))
				if jsb == nil {
					return errors.New("No jobs")
				}
				jb := jsb.Bucket(itob(work.GetPartOfID()))
				if jb == nil {
					log.Println("Job that we want to add work to does not exist AddWorks", work.GetPartOfID())
					return errors.New(fmt.Sprint("No job with ID ", work.GetPartOfID()))
				}
				workBucket, err := jb.CreateBucketIfNotExists([]byte("Parts"))
				if err != nil {
					return err
				}

				val := bytes.NewBuffer(jb.Get([]byte("NumParts")))
				var NumParts int64
				err = gob.NewDecoder(val).Decode(&NumParts)
				if err != nil {
					return err
				}

				NumParts++

				val = &bytes.Buffer{}
				err = gob.NewEncoder(val).Encode(NumParts)
				if err != nil {
					return err
				}
				jb.Put([]byte("NumParts"), val.Bytes())

				return SaveToBucket(workBucket, itob(work.GetPartID()), work)
			})
			if err != nil {
				errchan <- err
			}
		}(work)

	}
}

func (s *server) GetWorks(Id *databaseDefinition.ID, stream databaseDefinition.DataBase_GetWorksServer) error {

	return s.db.View(func(tx *bolt.Tx) error {
		jsb := tx.Bucket([]byte("jobs"))
		if jsb == nil {
			return errors.New("No jobs")
		}
		jb := jsb.Bucket(itob(Id.GetID()))
		if jb == nil {
			return errors.New(fmt.Sprint("Job does not exist", Id.GetID()))
		}

		workBucket := jb.Bucket([]byte("Parts"))
		if workBucket == nil {
			jb.ForEach(func(k, v []byte) error {
				log.Println(string(k))
				return nil
			})
			return errors.New(fmt.Sprint("workBucket does not exist", "Parts"))
		}

		return workBucket.ForEach(func(k, v []byte) error {
			work := &databaseDefinition.Work{}
			err := LoadFromBucket(workBucket, k, work)
			if err != nil {
				return err
			}
			return stream.Send(work)
		})

	})
}

func (s *server) GetJob(cntx context.Context, ID *databaseDefinition.ID) (*databaseDefinition.Job, error) {
	job := &databaseDefinition.Job{}
	return job, s.db.View(func(tx *bolt.Tx) error {
		jsb := tx.Bucket([]byte("jobs"))
		if jsb == nil {
			return errors.New("No jobs")
		}

		err := LoadFromBucket(jsb, itob(ID.GetID()), job)
		return err
	})
}

func (s *server) AddJob(cntx context.Context, job *databaseDefinition.Job) (*databaseDefinition.ID, error) {
	var ID *databaseDefinition.ID = &databaseDefinition.ID{}
	return ID, s.db.Batch(func(tx *bolt.Tx) error {
		jsb, err := tx.CreateBucketIfNotExists([]byte("jobs"))
		if err != nil {
			return err
		}
		id, err := jsb.NextSequence()
		if err != nil {
			return err
		}

		job.Id = int64(id)
		ID.ID = job.Id
		return SaveToBucket(jsb, itob(job.GetId()), job)
	})
}

func (s *server) DeleteJob(cntx context.Context, id *databaseDefinition.ID) (*google_protobuf.Empty, error) {
	return &google_protobuf.Empty{}, s.db.Batch(func(tx *bolt.Tx) error {
		jsb, err := tx.CreateBucketIfNotExists([]byte("jobs"))
		if err != nil {
			log.Println("tx.CreateBucketIfNotExists", err)
			return err
		}

		err = jsb.DeleteBucket(itob(id.GetID()))
		if err != nil {
			log.Println("jsb.Delete", err)
			return err
		}
		return err
	})
}

func (s *server) GetAll(userid *databaseDefinition.ID, stream databaseDefinition.DataBase_GetAllServer) error {
	return s.db.View(func(tx *bolt.Tx) error {
		jsb := tx.Bucket([]byte("jobs"))
		if jsb == nil {
			//There are no jobs to give, this is not really an error
			return nil
		}

		return jsb.ForEach(func(k, v []byte) error {
			jb := jsb.Bucket(k)
			val := bytes.NewBuffer(jb.Get([]byte("OwnerID")))
			var ownerID int64
			err := gob.NewDecoder(val).Decode(&ownerID)
			if err != nil {
				return err
			}

			if ownerID == userid.GetID() {
				job := &databaseDefinition.Job{}
				err := LoadFromBucket(jsb, k, job)
				if err != nil {
					fmt.Println("LoadFromBucket error", err)
					return err
				}
				err = stream.Send(job)

				if err != nil {
					return err
				}
			}
			return nil
		})

	})
}

func (s *server) JobDone(cntx context.Context, ID *databaseDefinition.ID) (*google_protobuf.Empty, error) {
	return &google_protobuf.Empty{}, s.db.Batch(func(tx *bolt.Tx) error {
		jsb := tx.Bucket([]byte("jobs"))
		if jsb == nil {
			//There are no jobs to give, this is not really an error
			return nil
		}

		job := &databaseDefinition.Job{}

		jb := jsb.Bucket(itob(ID.GetID()))
		if jb == nil {
			return errors.New("The job does not exist")
		}

		LoadFromBucket(jsb, itob(ID.GetID()), job)

		job.NumPartsDone++

		buf := &bytes.Buffer{}
		err := gob.NewEncoder(buf).Encode(job.NumPartsDone)
		if err != nil {
			return err
		}

		if job.NumParts == job.NumPartsDone {
			job.Done = true
		}

		return jb.Put([]byte("NumPartsDone"), buf.Bytes())

	})
}

func (s *server) GetPart(cntx context.Context, idid *databaseDefinition.JobWorkIdent) (*databaseDefinition.Work, error) {
	work := &databaseDefinition.Work{}
	return work, s.db.View(func(tx *bolt.Tx) error {
		jsb := tx.Bucket([]byte("jobs"))
		if jsb == nil {
			return errors.New("No jobs")
		}

		workBucket := jsb.Bucket(itob(idid.GetJobID())).Bucket([]byte("Parts"))

		return LoadFromBucket(workBucket, itob(idid.GetWorkID()), work)
	})
}

func (s *server) SaveWork(cntx context.Context, work *databaseDefinition.Work) (*google_protobuf.Empty, error) {
	return &google_protobuf.Empty{}, s.db.Batch(func(tx *bolt.Tx) error {
		jsb := tx.Bucket([]byte("jobs"))
		if jsb == nil {
			return errors.New("No jobs")
		}

		jb := jsb.Bucket(itob(work.GetPartOfID()))
		workBucket := jb.Bucket([]byte("Parts"))

		return SaveToBucket(workBucket, itob(work.GetPartID()), work)
	})
}

func (s *server) ResetStatus(context.Context, *google_protobuf.Empty) (*google_protobuf.Empty, error) {
	log.Println("Resetting status on all jobs")
	defer log.Println("Done Resetting status on all jobs")
	return &google_protobuf.Empty{}, s.db.Batch(func(tx *bolt.Tx) error {
		jsb := tx.Bucket([]byte("jobs"))
		if jsb == nil {
			return errors.New("No jobs")
		}

		return jsb.ForEach(func(jobKey, v []byte) error {
			jb := jsb.Bucket(jobKey)

			wb := jb.Bucket([]byte("Parts"))

			if wb == nil {
				return nil
			}
			return wb.ForEach(func(partKey, v []byte) error {

				buf := &bytes.Buffer{}
				err := gob.NewEncoder(buf).Encode(false)
				if err != nil {
					return err
				}
				part := wb.Bucket(partKey)

				return part.Put([]byte("Dispatched"), buf.Bytes())
			})
		})
	})
}

func SaveToBucket(b *bolt.Bucket, key []byte, value interface{}) error {
	val := reflect.ValueOf(value)
	typ := reflect.TypeOf(value)

	if typ.Kind() == reflect.Ptr {
		if val.IsNil() {
			//We can't save nothing
			return nil
		}

		val = reflect.Indirect(val)

		typ = val.Type()
	}

	if typ.Kind() == reflect.Struct {
		buc, err := b.CreateBucketIfNotExists(key)
		if err != nil {
			return err
		}
		for k := 0; k < val.NumField(); k++ {
			err := SaveToBucket(buc, []byte(typ.Field(k).Name), val.Field(k).Interface())
			if err != nil {
				return err
			}
		}
	} else if typ.Kind() == reflect.Array || typ.Kind() == reflect.Slice {
		buc, err := b.CreateBucketIfNotExists(key)
		if err != nil {
			return err
		}
		for k := 0; k < val.Len(); k++ {
			err := SaveToBucket(buc, itob(int64(k)), val.Index(k).Interface())
			if err != nil {
				return err
			}
		}
	} else {
		val := &bytes.Buffer{}
		err := gob.NewEncoder(val).Encode(value)
		if err != nil {
			return err
		}
		err = b.Put(key, val.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

func LoadFromBucket(b *bolt.Bucket, key []byte, value interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(value))
	typ := val.Type()

	if typ.Kind() == reflect.Struct {
		buc := b.Bucket(key)
		if buc == nil {
			//The origional array was nil
			return nil
		}
		for k := 0; k < val.NumField(); k++ {

			err := LoadFromBucket(buc, []byte(typ.Field(k).Name), val.Field(k).Addr().Interface())
			if err != nil {
				fmt.Println("Error Loading Field", typ.Field(k).Name, err)
				return err
			}
		}
	} else if typ.Kind() == reflect.Array || typ.Kind() == reflect.Slice {
		buc := b.Bucket(key)
		if buc == nil {
			//The origional array was nil
			return nil
		}
		err := buc.ForEach(func(k, v []byte) error {
			elm := val.Type().Elem()
			if elm.Kind() == reflect.Ptr {

				elm = elm.Elem()
			}

			loadInto := reflect.New(elm)
			err := LoadFromBucket(buc, k, loadInto.Interface())
			if err != nil {
				return err
			}
			if val.Type().Elem().Kind() != reflect.Ptr {

				loadInto = reflect.Indirect(loadInto)
			}
			val = reflect.Append(val, loadInto)
			return nil
		})

		if err != nil {
			return err
		}
		reflect.Indirect(reflect.ValueOf(value)).Set(val)

		return nil

	} else {
		v := b.Get(key)
		if v == nil {
			//The origional value was not saved
			return nil
		}
		val := bytes.NewBuffer(v)
		err := gob.NewDecoder(val).Decode(value)
		if err != nil {
			return err
		}

	}
	return nil
}

// itob returns an 8-byte big endian representation of v.
func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.PutVarint(b, v)
	return b
}

func btoi(v []byte) (k int64) {

	k, _ = binary.ReadVarint(bytes.NewBuffer(v))
	return
}
