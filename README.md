# raft

This is an implementation of the [Raft distributed consensus protocol][1].
It's heavily influenced by [benbjohnson's implementation][2].

[1]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[2]: https://github.com/benbjohnson/go-raft

## TODO

* ~~Leader election~~ _done_
* ~~Log replication~~ _done_
* ~~Basic unit tests~~ _done_
* Robust set of transports
* Configuration changes (joint-consensus mode)
* Log compaction
