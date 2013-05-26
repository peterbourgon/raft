# raft

This is an implementation of the [Raft distributed consensus protocol][paper].
It's heavily influenced by [benbjohnson's implementation][goraft].

[paper]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[goraft]: https://github.com/benbjohnson/go-raft

## TODO

* ~~Leader election~~ _done_
* ~~Log replication~~ _done_
* ~~Basic unit tests~~ _done_
* ~~HTTP transport~~ _done_
* [net/rpc][netrpc] transport
* Other transports?
* Configuration changes (joint-consensus mode)
* Log compaction
* Complex unit tests (one per scenario described in the paper)

[netrpc]: http://golang.org/pkg/net/rpc

