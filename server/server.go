package server

import (
  "bytes"
	"fmt"
	"github.com/goraft/raft"
	"stripe-ctf.com/sqlcluster/command"
  "stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/transport"
	"stripe-ctf.com/sqlcluster/util"
	"github.com/gorilla/mux"
	"io/ioutil"
	"stripe-ctf.com/sqlcluster/log"
	"math/rand"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type Server struct {
	name       string
	path       string
  listen     string
  connectionString string
	router     *mux.Router
	raftServer raft.Server
	httpServer *http.Server
	sql        *sql.SQL
	mutex      sync.RWMutex
	client     *transport.Client
}

// Creates a new server.
func New(path, listen string) (*Server, error) {
  cs, err := transport.Encode(listen)
  if err != nil {
  	return nil, err
  }

  sqlPath := filepath.Join(path, "storage.sql")
  util.EnsureAbsent(sqlPath)

	s := &Server{
		path:   path,
    listen: listen,
    connectionString: cs, 
		sql:    sql.NewSQL(sqlPath),
		router: mux.NewRouter(),
		client:  transport.NewClient(),
	}

	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(path, "name")); err == nil {
		s.name = string(b)
	} else {
		s.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}
  log.Println("My name is "+ s.name)
	return s, nil
}

// Starts the server.
func (s *Server) ListenAndServe(leader string) error {
	var err error

	log.Println("Initializing HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	s.router.HandleFunc("/sql", s.sqlHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	// Start Unix transport
	l, err := transport.Listen(s.listen)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Listening at:", s.connectionString)

	log.Printf("Initializing Raft Server: %s", s.path)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft")
  transporter.Transport.Dial = transport.UnixDialer
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.sql, "")
	if err != nil {
		log.Fatal(err)
	}
	transporter.Install(s.raftServer, s)
  s.raftServer.SetElectionTimeout(time.Duration(rand.Intn(200)+150) * time.Millisecond)
	s.raftServer.Start()

	if leader != "" {
		// Join to leader if specified.

		log.Println("Attempting to join leader:", leader)

		if !s.raftServer.IsLogEmpty() {
			log.Fatal("Cannot join with an existing log")
		}
		if err := s.Join(leader); err != nil {
			log.Fatal(err)
		}

	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.

		log.Println("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString,
		})
		if err != nil {
			log.Fatal(err)
		}

	} else {
		log.Println("Recovered from log")
	}

	return s.httpServer.Serve(l)
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Joins to the leader of an existing cluster.
func (s *Server) Join(leader string) error {

	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString,
	}

	cs, err := transport.Encode(leader)
	if err != nil {
		return err
	}

	for {
    //time.Sleep(time.Duration(rand.Intn(500)+100) * time.Millisecond)
    time.Sleep(100 * time.Millisecond)
  	b := util.JSONEncode(command)
		_, err := s.client.SafePost(cs, "/join", b)
		if err != nil {
			log.Printf("Unable to join cluster: %s", err)
			continue
		}
    log.Println("Successfully joined cluster")
		return nil
	}


	return nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := util.JSONDecode(req.Body, command); err != nil {
		log.Printf("Invalid join request: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Handling join request: %#v", command)

	if _, err := s.raftServer.Do(command); err != nil {
    log.Printf("Error executing join command: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
  log.Println("Successfully handler join request: ", command.Name)
}

func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	query := string(b)
  raftServer := s.raftServer
  leader     := raftServer.Leader()

  if raftServer.Name() != leader {
    
    wait := make(chan bool, 1)
    if _, ok := raftServer.Peers()[leader]; ! ok {
  		s.raftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
  			leader = e.Value().(string)
  			if leader != "" {
  				wait <- true
  			}
  		})
  		<- wait
    }
    
    if leaderPeer, ok := raftServer.Peers()[leader]; ok {
      leaderCS := leaderPeer.ConnectionString
      log.Printf("Attempting to proxy to primary: %v", leaderCS)
      resp, err := s.client.SafePost(leaderCS, "/sql", bytes.NewReader(b))
      if err != nil {
        http.Error(w, "Couldn't proxy response to primary: " + err.Error(), http.StatusServiceUnavailable)
        return
      }
      bytes, err := ioutil.ReadAll(resp)
      if err != nil {
        http.Error(w, "Couldn't proxy response to primary: " + err.Error(), http.StatusServiceUnavailable)
        return
      }
      log.Println("Proxied: ", string(bytes))
      w.Write(bytes)
      return
    } else {
      http.Error(w, "Unknown leader", http.StatusServiceUnavailable)
      return
    }
  }

	// Execute the command against the Raft server.
	response, err := s.raftServer.Do(command.NewQueryCommand(query))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
    return
	}
  
  w.Write(response.([]byte))
}
