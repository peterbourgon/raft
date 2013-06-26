package rafthttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/peterbourgon/raft"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
)

const (
	IdPath               = "/raft/id"
	AppendEntriesPath    = "/raft/appendentries"
	RequestVotePath      = "/raft/requestvote"
	CommandPath          = "/raft/command"
	SetConfigurationPath = "/raft/setconfiguration"
)

var (
	emptyAppendEntriesResponse bytes.Buffer
	emptyRequestVoteResponse   bytes.Buffer
)

func init() {
	json.NewEncoder(&emptyAppendEntriesResponse).Encode(raft.AppendEntriesResponse{})
	json.NewEncoder(&emptyRequestVoteResponse).Encode(raft.RequestVoteResponse{})
}

type Peer struct {
	id  uint64
	url url.URL
}

func NewPeer(u url.URL) (*Peer, error) {
	u.Path = ""

	idUrl := u
	idUrl.Path = IdPath
	resp, err := http.Get(idUrl.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	id, err := strconv.ParseUint(string(buf), 10, 64)
	if err != nil {
		return nil, err
	}
	if id <= 0 {
		return nil, fmt.Errorf("invalid peer ID %d", id)
	}

	return &Peer{
		id:  id,
		url: u,
	}, nil
}

func (p *Peer) Id() uint64 { return p.id }

func (p *Peer) AppendEntries(ae raft.AppendEntries) raft.AppendEntriesResponse {
	var aer raft.AppendEntriesResponse
	p.rpc(ae, AppendEntriesPath, &aer)
	return aer
}

func (p *Peer) RequestVote(rv raft.RequestVote) raft.RequestVoteResponse {
	var rvr raft.RequestVoteResponse
	p.rpc(rv, RequestVotePath, &rvr)
	return rvr
}

func (p *Peer) Command(cmd []byte, response chan []byte) error {
	go func() {
		var responseBuf bytes.Buffer
		p.rpc(cmd, CommandPath, &responseBuf)
		response <- responseBuf.Bytes()
	}()
	return nil // TODO could make this smarter (i.e. timeout), with more work
}

func (p *Peer) SetConfiguration(peers raft.Peers) error {
	return fmt.Errorf("not yet implemented")
}

func (p *Peer) rpc(request interface{}, path string, response interface{}) error {
	body := &bytes.Buffer{}
	if err := json.NewEncoder(body).Encode(request); err != nil {
		return err
	}

	url := p.url
	url.Path = path
	resp, err := http.Post(url.String(), "application/json", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		return err
	}

	return nil
}

type Server struct {
	server raft.Peer
}

func NewServer(server raft.Peer) *Server {
	return &Server{
		server: server,
	}
}

type Muxer interface {
	HandleFunc(string, func(http.ResponseWriter, *http.Request))
}

func (s *Server) Install(mux Muxer) {
	mux.HandleFunc(IdPath, s.idHandler())
	mux.HandleFunc(AppendEntriesPath, s.appendEntriesHandler())
	mux.HandleFunc(RequestVotePath, s.requestVoteHandler())
	mux.HandleFunc(CommandPath, s.commandHandler())
}

func (s *Server) idHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprint(s.server.Id())))
	}
}

func (s *Server) appendEntriesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var ae raft.AppendEntries
		if err := json.NewDecoder(r.Body).Decode(&ae); err != nil {
			http.Error(w, emptyAppendEntriesResponse.String(), http.StatusBadRequest)
			return
		}

		aer := s.server.AppendEntries(ae)
		if err := json.NewEncoder(w).Encode(aer); err != nil {
			http.Error(w, emptyAppendEntriesResponse.String(), http.StatusInternalServerError)
			return
		}
	}
}

func (s *Server) requestVoteHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var rv raft.RequestVote
		if err := json.NewDecoder(r.Body).Decode(&rv); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusBadRequest)
			return
		}

		rvr := s.server.RequestVote(rv)
		if err := json.NewEncoder(w).Encode(rvr); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusInternalServerError)
			return
		}
	}
}

func (s *Server) commandHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		// TODO unfortunately, we squelch a lot of errors here.
		// Maybe there's a way to report different classes of errors
		// than with an empty response.

		cmd, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		response := make(chan []byte, 1)
		if err := s.server.Command(cmd, response); err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		resp, ok := <-response
		if !ok {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		w.Write(resp)
	}
}
