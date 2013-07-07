package raft

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
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
	// TODO gob register the peer/s
	json.NewEncoder(&emptyAppendEntriesResponse).Encode(AppendEntriesResponse{})
	json.NewEncoder(&emptyRequestVoteResponse).Encode(RequestVoteResponse{})
}

// TODO
type HTTPTransport struct{}

// TODO
func (t *HTTPTransport) Register(mux *http.ServeMux, s *Server) {
	mux.HandleFunc(IdPath, t.idHandler(s))
	mux.HandleFunc(AppendEntriesPath, t.appendEntriesHandler(s))
	mux.HandleFunc(RequestVotePath, t.requestVoteHandler(s))
	mux.HandleFunc(CommandPath, t.commandHandler(s))
	mux.HandleFunc(SetConfigurationPath, t.setConfigurationHandler(s))
}

func (t *HTTPTransport) idHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprint(s.Id())))
	}
}

func (t *HTTPTransport) appendEntriesHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var ae AppendEntries
		if err := json.NewDecoder(r.Body).Decode(&ae); err != nil {
			http.Error(w, emptyAppendEntriesResponse.String(), http.StatusBadRequest)
			return
		}

		aer := s.AppendEntries(ae)
		if err := json.NewEncoder(w).Encode(aer); err != nil {
			http.Error(w, emptyAppendEntriesResponse.String(), http.StatusInternalServerError)
			return
		}
	}
}

func (t *HTTPTransport) requestVoteHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var rv RequestVote
		if err := json.NewDecoder(r.Body).Decode(&rv); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusBadRequest)
			return
		}

		rvr := s.RequestVote(rv)
		if err := json.NewEncoder(w).Encode(rvr); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusInternalServerError)
			return
		}
	}
}

func (t *HTTPTransport) commandHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		// TODO we're collapsing a lot of errors into an empty response.
		// If we can decide on an error format, we could propegate them.

		cmd, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		response := make(chan []byte, 1)
		if err := s.Command(cmd, response); err != nil {
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

func (t *HTTPTransport) setConfigurationHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var peers Peers
		if err := gob.NewDecoder(r.Body).Decode(&peers); err != nil {
			errBuf, _ := json.Marshal(commaError{err.Error(), false})
			http.Error(w, string(errBuf), http.StatusBadRequest)
			return
		}

		if err := s.SetConfiguration(peers); err != nil {
			errBuf, _ := json.Marshal(commaError{err.Error(), false})
			http.Error(w, string(errBuf), http.StatusInternalServerError)
			return
		}

		respBuf, _ := json.Marshal(commaError{"", true})
		w.Write(respBuf)
	}
}

// commaError is the structure returned by the configuration handler, to clients
// that make set-configuration requests over the HTTP Transport.
type commaError struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success,omitempty"`
}

//
//
//

type HTTPPeer struct {
	id  uint64
	url url.URL
}

// NewHTTPPeer constructs a new HTTP peer. Part of construction involves
// making a HTTP GET requests against the IdPath to resolve the remote node's
// ID.
func NewHTTPPeer(u url.URL) (*HTTPPeer, error) {
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

	return &HTTPPeer{
		id:  id,
		url: u,
	}, nil
}

func (p *HTTPPeer) Id() uint64 { return p.id }

func (p *HTTPPeer) AppendEntries(ae AppendEntries) AppendEntriesResponse {
	var aer AppendEntriesResponse
	p.rpc(ae, AppendEntriesPath, &aer)
	return aer
}

func (p *HTTPPeer) RequestVote(rv RequestVote) RequestVoteResponse {
	var rvr RequestVoteResponse
	p.rpc(rv, RequestVotePath, &rvr)
	return rvr
}

func (p *HTTPPeer) Command(cmd []byte, response chan []byte) error {
	go func() {
		var responseBuf bytes.Buffer
		p.rpc(cmd, CommandPath, &responseBuf)
		response <- responseBuf.Bytes()
	}()
	return nil // TODO could make this smarter (i.e. timeout), with more work
}

func (p *HTTPPeer) SetConfiguration(peers Peers) error {
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(&peers); err != nil {
		return err
	}

	var resp commaError
	p.rpc(buf, SetConfigurationPath, &resp)
	if !resp.Success {
		return fmt.Errorf(resp.Error)
	}

	return nil
}

func (p *HTTPPeer) rpc(request interface{}, path string, response interface{}) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(request); err != nil {
		return err
	}

	url := p.url
	url.Path = path
	resp, err := http.Post(url.String(), "application/json", &body)
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
