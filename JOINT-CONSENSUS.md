## Description

This document describes the rationale behind the joint-consensus implementation,
as a literal analaysis of section 6 (Cluster membership changes) of the
[Raft paper](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).
Each sentence in the section is indexed, and parsed for requirements,
suggestions, and hints to the implementation.


## Analysis

> **1** In Raft the cluster first switches to a transitional configuration we call
> (joint consensus; once the joint consensus has been committed, the system then
> (transitions to the new configuration.

Every server should probably have a two-phase approach to configuration
management: either in a stable (Cold/Cnew) or unstable (Cold,new) state.

```
type Configuration struct { stable, unstable Peers }
```


> **2** The joint consensus combines both the old and new configurations:

> (2a) Log entries are replicated to all servers in both configurations.

Being in the unstable Cold,new state means sending requests to a union-set of
Cold and Cnew servers.

```
func (c *Configuration) AllPeers() Peers { /* union-set */ }
```


> (2b) Any server from either configuration may serve as leader.

(statement of fact)


> (2c) Agreement (for elections and entry commitment) requires majorities from
> both the old and new configurations.

Being in the unstable Cold,new state means changes must be independently
validated by a majority of Cold servers AND Cnew servers.

```
	Cold        Cnew        Result          Pass?
	A B C       A B C D     A+ B+ C- D-     Cold+ Cnew- => FAIL
	A B C       A B C D     A+ B+ C- D+     Cold+ Cnew+ => pass
	A B C       A B C D     A+ B+ C+ D-     Cold+ Cnew+ => pass
```

```
func (c *Configuration) Pass(votes map[uint64]bool) bool { ... }
```

So far: in Cold,new, membership in Cold or Cnew is notable only to the
leader, for the purposes of vote-counting.


> **3** As will be shown below, the joint consensus allows individual servers to
> transition between configurations at different times without compromising
> safety.

(statement of promise)


> **4** Furthermore, joint consensus allows the cluster to continue servicing
> client requests throughout the configuration change.

Client requests (i.e. commands) should always be accepted, regardless of
joint-consensus state.


> **5** Cluster configurations are stored and communicated using special entries
> in the replicated log; Figure 9 illustrates the configuration change process.

Configurations need to be communicated through AppendEntries RPCs.
Configurations need to be marshalable/unmarshalable.
Implying: peers need to be marshalable/unmarshalable.


> **6** When the leader receives a request to change the configuration from Cold
> to Cnew, it stores the configuration for joint consensus (Cold,new in the
> figure) as a log entry and replicates that entry using the mechanisms
> described previously.

A request to change configuration is forwarded to and dispatched by the
leader, just like a user command. Followers intercept the configuration-change
command and use it to manipulate their configuration.

handleAppendEntries should attempt to unmarshal the command as a Configuration
change. Followers don't propegate the configuration-change command to the user
state machine.


> **7** Once a given server adds the new configuration entry to its log, it uses
> that configuration for all future decisions (it does not wait for the entry to
> become committed).

A follower immediately applies a received configuration-change command. A
follower can't leverage the apply() functionality as a mechanism to apply
configuration changes. **This is currently being violated**.


> **8** This means that the leader will use the rules of Cold,new to determine
> when the log entry for Cold,new is committed.

A leader immediately applies a received configuration-change command.
Assuming it's not already in an unstable state (reject otherwise).


> **9** If the leader crashes, a new leader may be chosen under either Cold or
> Cold,new, depending on whether the winning candidate has received Cold,new.

(statement of fact)


> **10** In any case, Cnew cannot make unilateral decisions during this period.

(statement of fact)


> **11** Once Cold,new has been committed, neither Cold nor Cnew can make
> decisions without approval of the other, and the Leader Log Property ensures
> that only servers with the Cold,new log entry can be elected as leader.

(statement of fact)


> **12** It is now safe for the leader to create a log entry describing Cnew and
> replicate it to the cluster.

Once the configuration-change command from Cold to Cold,new has been committed,
the leader transitions from Cold,new to Cnew. The standard apply() mechanism
could be leveraged for this behavior.

Implementation: Configuration-change entry gets custom apply() that changes
leader Configuration.


> **13** Again, this configuration will take effect on each server as soon as it
> is seen.

(statement of fact)


> **14** When the new configuration has been committed under the rules of Cnew,
> the old configuration is irrelevant and servers not in the new configuration
> can be shut down.

If a server receives a configuration-change command that results in a
configuration that doesn't include its server ID, when that command is
committed, it should shut itself down. **This is currently being violated**.

Implementation: special case in custom apply().


> **15** As shown in Figure 9, there is no time when Cold and Cnew can both make
> unilateral decisions; this guarantees safety.

(statement of fact)


> **16** There are two more issues to address for reconfiguration.

(statement of promise)


> **17** First, if the leader is part of Cold but not part of Cnew, it must
> eventually step down.

(prefix of 18)


> **18** In Raft the leader steps down immediately after committing a
> configuration entry that does not include itself.

A leader that finds it's not a part of a new configuration change should step
down immediately after committing that change, but not before. The apply()
mechanism could be leveraged for this behavior. See **12**. Stepping down can by
synonymous with exiting its select loop -- the same behavior as followers.


> **19** This means that there will be a period of time (while it is committing
> Cnew) where the leader is managing a cluster that does not include itself; it
> replicates log entries but does not count itself in majorities.

Leaders should take care to not automatically vote for themselves when
counting majorities for a [Cnew] state that doesn't include them.
**I believe this is currently being violated**.


> **20** The leader should not step down earlier, because members not in Cnew
> could still be elected, resulting in unnecessary elections.

(statment of fact)


> **21** The second issue is that new servers may not initially store any log
> entries.

(statement of fact)


> **22** If they are added to the cluster in this state, it could take quite a
> while for them to catch up, during which time it might not be possible to
> commit new log entries.

(statement of fact)


> **23** In order to avoid availability gaps, Raft introduces an additional
> phase before the configuration change, in which the new servers join the
> cluster as non-voting members (the leader will replicate log entries to them,
> but they are not considered for majorities).

I believe this behavior is underspecified, and cannot be reliably implemented
without some kind of sentinel value that's a first-order property of a peer.
Specifically, the case when the leader crashes during this catch-up phase is
not clear to me. Therefore, **I don't implement this functionality** and won't
until I can get it straight in my head.


> **24** Once the new servers’ logs have caught up with the rest of the cluster,
> the reconfiguration can proceed as described above.

See above.