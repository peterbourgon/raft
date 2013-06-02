# raft

This is an implementation of the [Raft distributed consensus protocol][paper].
It's heavily influenced by [benbjohnson's implementation][goraft].

[paper]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[goraft]: https://github.com/benbjohnson/go-raft

## Usage

A node in a Raft network is represented by a [Server][server] component. In a
typical application, nodes will create and start a Server, and expose it to
other nodes using a [Peer][peer] interface.

[server]: http://godoc.org/github.com/peterbourgon/raft#Server
[peer]: http://godoc.org/github.com/peterbourgon/raft#Peer

The core raft package includes a [LocalPeer][localpeer] wrapper, which provides
a Peer interface to a Server within the same process-space. This is intended
for testing and demonstration purposes.

[localpeer]: http://godoc.org/github.com/peterbourgon/raft#LocalPeer

For real-life applications, you'll probably want to expose your Server via some
kind of network transport. This library includes a [HTTP transport][http] which
exposes a Raft server via REST-ish endpoints. For now, it's the simplest way to
embed a Raft server in your application.

[http]: http://godoc.org/github.com/peterbourgon/raft/http

```go
import (
	"github.com/peterbourgon/raft"
	"github.com/peterbourgon/raft/http"
	"net/http"
)

// This callback should apply the passed command to your state machine.
func apply(commitIndex uint64, cmd []byte) []byte {
	return []byte{}
}

func main() {
	// Create the Raft server
	id := 123
	store := &bytes.Buffer{}
	raftServer := raft.NewServer(id, store, apply)
	raftServer.SetConfiguration(peers) // from some external source

	// Create the HTTP server
	mux := http.NewServeMux()
	httpServer := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	// Create and install the HTTP/Raft transport bridge
	rafthttpServer := rafthttp.NewServer(raftServer)
	rafthttpServer.Install(mux)

	// Start everything
	go httpServer.ListenAndServe()
	raftServer.Start()
	defer raftServer.Stop()

	// Now you can issue commands.
	raftServer.Command([]byte(`SET VALUE 100`))

	// When the command is successfully (safely) replicated, the `apply`
	// callback will get triggered, and you'll update your state machine.

	select {}
}
```

Several other transport bridges are coming: net/rpc, straight TCP (probably),
protobufs (maybe), etc.


## TODO

* ~~Leader election~~ _done_
* ~~Log replication~~ _done_
* ~~Basic unit tests~~ _done_
* ~~HTTP transport~~ _done_
* [net/rpc][netrpc] transport
* Other transports?
* Configuration changes (joint-consensus mode) ☜ **up next**
* Log compaction
* Complex unit tests (one per scenario described in the paper)

[netrpc]: http://golang.org/pkg/net/rpc

