package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/HeinOldewage/Hyades"
	//"github.com/gorilla/context"

	"html/template"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/sessions"
)

type ConfigFile struct {
	DataPath      *string
	ServerAddress *string
	DB            *string
}

var configFilePath *string = flag.String("config", "config.json", "If the config file is specified it overrides command line paramters and defaults")

var configuration ConfigFile = ConfigFile{
	DataPath:      flag.String("dataFolder", "userData", "The folder that the distribution server saves the data"),
	ServerAddress: flag.String("address", ":8088", "The folder that the distribution server saves the data"),
	DB:            flag.String("DBFile", "db.sql", "Sqlite db file"),
}

func main() {
	fmt.Println("This is the web server")
	flag.Parse()

	if *configFilePath != "" {
		fmt.Println("Loading config file", *configFilePath)
		file, err := os.Open(*configFilePath)
		if err != nil {
			log.Println(err)
			return
		}

		decoder := json.NewDecoder(file)
		err = decoder.Decode(&configuration)
		if err != nil {
			log.Println(err)
			return
		}
	}

	log.Println("config", *configuration.DataPath, *configuration.DB, *configuration.ServerAddress)

	log.Println("Starting web server on ", *configuration.ServerAddress)
	submitServer := NewSubmitServer(*configuration.ServerAddress, *configuration.DB)
	submitServer.Listen()
}

const usersFileName string = "users.gob"

type SubmitServer struct {
	Address        string
	JobServer      interface{}
	Cookiestore    *sessions.CookieStore
	sessionUserMap map[string]*Hyades.Person

	jobs  *JobMap
	users *UserMap
}

func NewSubmitServer(Address string, dbFile string) *SubmitServer {

	//Delete all previous Jobs, After Users are saved/loaded from file only delete if that fails

	userMap := NewUserMap(dbFile)

	defer log.Println("NewSubmitServer Done")
	return &SubmitServer{Address,
		nil,
		sessions.NewCookieStore([]byte("ForTheUnity")),
		make(map[string]*Hyades.Person),
		NewJobMap(dbFile),
		userMap,
	}

}

func (ss *SubmitServer) Listen() {

	http.HandleFunc("/submit", ss.securePage(ss.submitJob))
	http.HandleFunc("/stop", ss.securePage(ss.stopJob))
	http.HandleFunc("/start", ss.securePage(ss.startJob))
	http.HandleFunc("/Jobs", ss.securePage(ss.listJobs))
	http.HandleFunc("/JobStatus", ss.securePage(ss.jobStatus))
	http.HandleFunc("/GetJobResult", ss.securePage(ss.getJobResult))
	http.HandleFunc("/CreateJob", ss.securePage(ss.createJob))
	http.HandleFunc("/Help", ss.securePage(ss.help))
	http.HandleFunc("/About", ss.securePage(ss.about))
	http.HandleFunc("/Admin", ss.securePage(ss.admin))
	http.HandleFunc("/Logout", ss.securePage(ss.logoutUser))

	http.HandleFunc("/Observe/Get/", ss.securePage(ss.observe))

	http.HandleFunc("/Observe/New/", ss.securePage(ss.addObserver))

	http.HandleFunc("/TryLogin", ss.loginUser)
	http.HandleFunc("/TryRegister", ss.newUser)

	http.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir("resources/files"))))

	log.Println("Starting SubmitServer")
	err := http.ListenAndServe(ss.Address, nil)
	if err != nil {
		panic(err)
	}
}

func (ss *SubmitServer) submitJob(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	Env, Envfh, _ := req.FormFile("Env")
	descr, _, _ := req.FormFile("workDescr")
	if !(Env == nil || Envfh == nil) && descr != nil {

		descrReader := bufio.NewReader(descr)

		job := ss.Jobs().NewJob(user)

		decodeError := json.NewDecoder(descrReader).Decode(job)
		log.Println("Creating job for user with id", user.Id, " And name", user.Username)
		job.OwnerID = user.Id
		job.JobFolder = user.Username

		if decodeError != nil {
			http.Error(w, decodeError.Error(), http.StatusBadRequest)
			return
		}

		//Save envBytes to file
		filename := filepath.Join(*configuration.DataPath, "EnvFiles", user.Username, job.Name+"env.zip")
		file, err := os.Create(filename)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer file.Close()
		_, err = io.Copy(file, Env)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		job.Env = filename

		log.Println("About to call ss.Jobs().AddJob(job)")
		err = ss.Jobs().AddJob(job)
		if err != nil {
			log.Println("ss.Jobs().AddJob(job):", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Println("Job created")
	} else {

		log.Println("File not correctly uploaded")
	}
}

func GetSubject(val reflect.Value, path []string) (interface{}, error) {
	if val.Type().Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	}

	parts := strings.Split(path[0], "*")
	element := parts[0]
	var query []string
	if len(parts) > 1 {
		query = parts[1:]
	}
	Fieldval := val.FieldByName(element)
	if Fieldval.IsValid() {
		val = Fieldval
	} else {
		//Maybe it is not a field
		MethVal := val.MethodByName(element)
		if !MethVal.IsValid() {
			for k := 0; k < val.Type().NumField(); k++ {
				log.Println(val.Type().Field(k).Name)
			}
			for k := 0; k < val.Type().NumMethod(); k++ {
				log.Println(val.Type().Method(k).Name)
			}
			return nil, errors.New(fmt.Sprint("Could not find a field or method with the name ", element, " on ", val.Type()))
		} else {
			val = MethVal
		}
		if val.Type().NumOut() == 0 {
			return nil, errors.New(fmt.Sprint("Function of type", val.Type(), " does not return a value"))
		}
		if val.Type().NumIn() == 0 {
			val = val.Call([]reflect.Value{})[0]
		}
	}

	if len(query) != 0 {
		switch val.Type().Kind() {
		case reflect.Slice, reflect.Array:
			{
				index, err := strconv.Atoi(query[0])
				if err != nil {
					return nil, err
				}
				val = val.Index(index)
			}
		case reflect.Map:
			{
				var key reflect.Value
				switch val.Type().Key().Kind() {
				case reflect.Int:
					{
						i, err := strconv.Atoi(query[0])
						if err != nil {
							return nil, err
						}
						key = reflect.ValueOf(i)
					}
				case reflect.String:
					{
						key = reflect.ValueOf(query[0])
					}
				}
				val = val.MapIndex(key)
				if !val.IsValid() {
					return nil, errors.New(fmt.Sprint("Map does not contain ", key.Interface()))
				}
			}
		}

	}

	if len(path) > 1 {
		return GetSubject(val, path[1:])
	} else {
		if val.CanInterface() {
			res := val.Interface()
			return res, nil
		} else {
			return nil, errors.New(fmt.Sprint("Cannot access ", val.Type().Name()))
		}
	}
}

func (ss *SubmitServer) addObserver(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	path := strings.Split(req.URL.Path, "/")
	path = path[3:]
	log.Println(path)

	val, err := GetSubject(reflect.ValueOf(ss), path)
	if err != nil {
		json.NewEncoder(w).Encode(err.Error())
		return
	}
	subject, ok := val.(Hyades.Observable)
	if !ok {
		fmt.Fprintf(w, "Object (%T) not observable", val)
		return
	}
	observer := subject.AddObserver()
	json.NewEncoder(w).Encode(observer.Id)

}

func (ss *SubmitServer) observe(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	id, ok := req.Form["id"]
	if !ok {
		log.Println(req.Form)
		http.Error(w, "Observer id not provided", http.StatusNotFound)
		return
	}
	obID, err := strconv.Atoi(id[0])
	if err != nil {
		log.Println(id)
		http.Error(w, "Observer id malformed"+id[0], http.StatusNotFound)
		return

	}

	path := strings.Split(req.URL.Path, "/")
	path = path[3:]
	log.Println(path)

	val, err := GetSubject(reflect.ValueOf(ss), path)
	if err != nil {
		json.NewEncoder(w).Encode(err.Error())
		return
	}
	subject, ok := val.(Hyades.Observable)
	if !ok {
		fmt.Fprintln(w, "Object not observable")
		return
	}

	changes, ok := subject.GetChanges(uint32(obID))
	if !ok {
		http.Error(w, "Observer does not exist", http.StatusNotFound)
		return

	}
	json.NewEncoder(w).Encode(changes)

}

func (ss SubmitServer) Jobs() *JobMap {
	return ss.jobs
}

func (ss *SubmitServer) stopJob(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	id, ok := req.Form["id"]
	if !ok {
		log.Println(req.Form)
		http.Error(w, "Job id not provided", http.StatusNotFound)
		return
	}
	log.Println("Stopping", id[0])

	//TODO :: Notify
	/*	if job, ok := ss.jobs.GetJob(id[0]); ok {

			//ss.JobServer.StopJob(job)
		} else {
			log.Println("Failed to find job", id[0], "in map")

	}*/

}

func (ss *SubmitServer) startJob(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	id, ok := req.Form["id"]
	if !ok {
		log.Println(req.Form)
		http.Error(w, "Job id not provided", http.StatusNotFound)
		return
	}
	log.Println("Starting", id[0])

	//TODO :: Notify
	/*
		if job, ok := ss.jobs.GetJob(id[0]); ok {

			//ss.JobServer.StartJob(job)
		} else {
			log.Println("Failed to find job", id[0], "in map")

		}*/
}

func (ss *SubmitServer) createJob(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	var fm template.FuncMap = make(template.FuncMap)

	fm["currentTab"] = func() string {
		return "createJob"
	}
	jobsTemplate, err := template.New("frame.html").Funcs(fm).ParseFiles("resources/templates/frame.html", "resources/templates/createJobs/header.html",
		"resources/templates/nav.html", "resources/templates/createJobs/body.html")
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pageData := map[string]interface{}{"NavData": user, "HeaderData": nil, "BodyData": nil}
	err = jobsTemplate.Execute(w, pageData)

	if err != nil {
		log.Println(err)
	}
}

func (ss *SubmitServer) listJobs(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	var fm template.FuncMap = make(template.FuncMap)

	fm["IDToString"] = func(id int) string {
		return strconv.Itoa(id)
	}

	fm["CountDone"] = func(id string) string {

		job, err := ss.jobs.GetJob(string(id))
		if err != nil {
			log.Println("listJobs_CountDone", err, id)
			return ""
		}
		return fmt.Sprint(job.NumPartsDone())
	}
	fm["totalWork"] = func(id string) string {

		job, err := ss.jobs.GetJob(string(id))
		if err != nil {
			log.Println("listJobs_totalWork", err, id)
			return ""
		}
		return fmt.Sprint(len(job.Parts))
	}

	fm["currentTab"] = func() string {
		return "listJobs"
	}

	jobsTemplate, err := template.New("frame.html").Funcs(fm).ParseFiles("resources/templates/frame.html", "resources/templates/listJobs/body.html",
		"resources/templates/listJobs/listJob.html", "resources/templates/listJobs/header.html", "resources/templates/nav.html")
	if err != nil {
		log.Println("Template parse error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jobs, err := ss.jobs.GetAll(user)

	if err != nil {
		log.Println("ss.jobs.GetAll(user)", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pageData := map[string]interface{}{"NavData": user, "HeaderData": nil, "BodyData": jobs}
	err = jobsTemplate.Execute(w, pageData)

	if err != nil {
		log.Println("jobsTemplate.Execute", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (ss *SubmitServer) jobStatus(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	id, ok := req.Form["id"]
	if !ok {
		log.Println(req.Form)
		http.Error(w, "Job id not provided", http.StatusNotFound)
		return
	}

	var fm template.FuncMap = make(template.FuncMap)
	fm["IDToString"] = func(id string) string {

		return id
	}

	fm["CountDone"] = func(id string) string {

		job, err := ss.jobs.GetJob(string(id))
		if err != nil {
			log.Println("listJobs_CountDone", err, id)
			return ""
		}
		return fmt.Sprint(job.NumPartsDone())
	}
	fm["currentTab"] = func() string {
		return ""
	}

	jobsTemplate, err := template.New("frame.html").Funcs(fm).ParseFiles("resources/templates/frame.html", "resources/templates/jobStatus/header.html",
		"resources/templates/jobStatus/body.html", "resources/templates/jobStatus/statusWork.html", "resources/templates/nav.html")
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	job, err := ss.jobs.GetJob(string(id[0]))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pageData := map[string]interface{}{"NavData": user, "HeaderData": job, "BodyData": job}
	err = jobsTemplate.Execute(w, pageData)

	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (ss *SubmitServer) getJobResult(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	id, ok := req.Form["id"]
	if !ok {
		http.Error(w, "Job id not provided", http.StatusNotFound)
		return
	}

	job, err := ss.jobs.GetJob(id[0])
	if err != nil {
		log.Println("id[0]", id[0])
		log.Println("getJobResult - ss.jobs.GetJob", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("job.JobFolder", job.JobFolder)
	log.Println("user.Username", user.Username)

	TempJobFolder := filepath.Join(*configuration.DataPath, job.JobFolder, job.Name)

	zipedfilePath := filepath.Join(*configuration.DataPath, job.JobFolder, "Job"+job.Name+".zip")
	log.Println("Creating zip at", zipedfilePath)
	zipedFile, err := os.OpenFile(zipedfilePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		log.Println("Error creating file", zipedfilePath, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stat, err := zipedFile.Stat()
	if err == nil && stat.Size() == 0 {
		log.Println("About to compress", TempJobFolder)
		Hyades.ZipCompressFolderWriter(TempJobFolder, zipedFile)
		log.Println("Zipped file")
	}

	http.ServeContent(w, req, "Job"+job.Name+".zip", time.Now(), zipedFile)

}

func (ss *SubmitServer) help(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	var fm template.FuncMap = make(template.FuncMap)

	fm["currentTab"] = func() string {
		return "help"
	}

	jobsTemplate, err := template.New("frame.html").Funcs(fm).ParseFiles("resources/templates/frame.html", "resources/templates/help/body.html",
		"resources/templates/help/header.html", "resources/templates/nav.html")
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pageData := map[string]interface{}{"NavData": user, "HeaderData": nil, "BodyData": ss.jobs}
	err = jobsTemplate.Execute(w, pageData)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (ss *SubmitServer) about(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	var fm template.FuncMap = make(template.FuncMap)
	fm["currentTab"] = func() string {
		return "about"
	}

	jobsTemplate, err := template.New("frame.html").Funcs(fm).ParseFiles("resources/templates/frame.html", "resources/templates/about/body.html",
		"resources/templates/about/header.html", "resources/templates/nav.html")
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pageData := map[string]interface{}{"NavData": user, "HeaderData": nil, "BodyData": nil}
	err = jobsTemplate.Execute(w, pageData)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (ss *SubmitServer) admin(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {
	if !user.Admin {
		http.Error(w, "404 page not found", http.StatusNotFound)
		return
	}
	var fm template.FuncMap = make(template.FuncMap)
	fm["currentTab"] = func() string {
		return "admin"
	}

	jobsTemplate, err := template.New("frame.html").Funcs(fm).ParseFiles("resources/templates/frame.html", "resources/templates/admin/body.html",
		"resources/templates/admin/header.html", "resources/templates/nav.html")
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pageData := map[string]interface{}{"NavData": user, "HeaderData": nil, "BodyData": ss.jobs, "Users": ss.users}
	err = jobsTemplate.Execute(w, pageData)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (ss *SubmitServer) logoutUser(user *Hyades.Person, w http.ResponseWriter, req *http.Request) {

	session, err := ss.Cookiestore.Get(req, "Session")
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)

		return
	}

	session.Values["sessID"] = ""
	session.Save(req, w)

	javascriptredirect(w, "/")
}

func (ss *SubmitServer) newUser(w http.ResponseWriter, req *http.Request) {
	session, err := ss.Cookiestore.Get(req, "Session")

	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	req.ParseForm()
	if len(req.PostForm["Name"]) == 0 || len(req.PostForm["Password"]) == 0 {
		log.Println("Name or password missing:", req.PostForm)
		http.Error(w, "Name or password missing", http.StatusUnauthorized)
		return
	}

	u, ok := ss.users.addUser(req.PostForm["Name"][0], req.PostForm["Password"][0])

	if ok {
		log.Println("New user added")

		sessID := strconv.FormatInt(time.Now().Unix(), 10)
		session.Values["sessID"] = sessID

		ss.sessionUserMap[sessID] = u

		log.Println("!!New session!!")
	} else {
		log.Println("!!Username already in use!!")
		http.Error(w, "Username already in use", http.StatusUnauthorized)
	}
	session.Save(req, w)
}

func (ss *SubmitServer) loginUser(w http.ResponseWriter, req *http.Request) {
	session, err := ss.Cookiestore.New(req, "Session")

	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)

		return
	}
	req.ParseForm()
	if len(req.PostForm["Name"]) == 0 || len(req.PostForm["Password"]) == 0 {
		log.Println("Name or password missing:", req.PostForm)
		http.Error(w, "Name or password missing", http.StatusUnauthorized)
		return
	}

	u, ok := ss.users.findUser(req.PostForm["Name"][0], req.PostForm["Password"][0])

	if ok {

		sessID := strconv.FormatInt(time.Now().Unix(), 10)
		session.Values["sessID"] = sessID

		ss.sessionUserMap[sessID] = u

		log.Println("!!New session!!")
	} else {
		log.Println("!!Invalid username/password on login!!")
		http.Error(w, "Not a valid username or password", http.StatusUnauthorized)
	}
	session.Save(req, w)
}

func (ss *SubmitServer) securePage(toRun func(runuser *Hyades.Person, w http.ResponseWriter, req *http.Request)) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		session, err := ss.Cookiestore.Get(req, "Session")
		if err != nil {
			log.Println("securePage", req.URL.Path, "err:", err)
		}
		if SessIDut, ok := session.Values["sessID"]; ok {

			var SessID string
			switch t := SessIDut.(type) {
			case string:
				SessID = t //SessIDut.(string)
			default:
				http.Error(w, "SessID invalid type", http.StatusInternalServerError)
			}

			runuser, ok := ss.sessionUserMap[SessID]
			if ok {
				toRun(runuser, w, req)
			} else {
				javascriptredirect(w, "/?to="+req.URL.RequestURI())
				//http.Error(w, "/", http.StatusTemporaryRedirect )

			}

		} else {
			javascriptredirect(w, "/?to="+req.URL.RequestURI())
			//http.Error(w, "/", http.StatusTemporaryRedirect )
		}
		session.Save(req, w)
	}

}

func javascriptredirect(w io.Writer, path string) {
	writer := bufio.NewWriter(w)
	writer.WriteString("<!DOCTYPE html><html><script type=\"text/javascript\" >")
	writer.WriteString("location.assign(\"" + path + "\") </script></html>\n")
	err := writer.Flush()
	if err != nil {
		log.Println("javascriptredirect", err)
	}
}
