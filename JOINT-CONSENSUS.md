## Description

This document describes the rationale behind the joint-consensus implementation,
as a literal analaysis of section 6 (Cluster membership changes) of the
[Raft paper](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).
Each sentence in the section is indexed, and parsed for requirements,
suggestions, and hints to the implementation.


## Analysis

> **1** In Raft the cluster first switches to a transitional configuration we
> call joint consensus; once the joint consensus has been committed, the system
> then transitions to the new configuration.

Every server should probably have a two-phase approach to configuration
management: either in a stable (C_old) or unstable (C_old + C_old,new) state.


> **2** The joint consensus combines both the old and new configurations:
> **2a** Log entries are replicated to all servers in both configurations.

Being in the unstable C_old,new state means sending requests to a union-set of
C_old and C_new servers.


> **2b** Any server from either configuration may serve as leader.

_statement of fact_


> **2c** Agreement (for elections and entry commitment) requires majorities from
> both the old and new configurations.

Being in the unstable C_old,new state means changes must be independently
validated by a majority of C_old servers AND C_new servers.

```
	C_old       C_new       Result          Pass?
	A B C       A B C D     A+ B+ C- D-     C_old+ C_new- => FAIL
	A B C       A B C D     A+ B+ C- D+     C_old+ C_new+ => pass
	A B C       A B C D     A+ B+ C+ D-     C_old+ C_new+ => pass
```

```
func (c *Configuration) Pass(votes map[uint64]bool) bool { ... }
```

So far: in C_old,new, membership in C_old or C_new is notable only to the
leader, for the purposes of vote-counting.


> **3** As will be shown below, the joint consensus allows individual servers to
> transition between configurations at different times without compromising
> safety.

_statement of promise_


> **4** Furthermore, joint consensus allows the cluster to continue servicing
> client requests throughout the configuration change.

Client requests (i.e. commands) should always be accepted, regardless of
joint-consensus state.


> **5** Cluster configurations are stored and communicated using special entries
> in the replicated log; Figure 9 illustrates the configuration change process.

Configurations need to be communicated through AppendEntries RPCs.
Configurations need to be marshalable/unmarshalable.
Implying: peers need to be marshalable/unmarshalable.


> **6** When the leader receives a request to change the configuration from
> C_old to C_new, it stores the configuration for joint consensus (C_old,new in
> the figure) as a log entry and replicates that entry using the mechanisms
> described previously.

A request to change configuration is forwarded to and dispatched by the
leader, just like a user command. Followers intercept the configuration-change
command and use it to manipulate their configuration.

handleAppendEntries should attempt to unmarshal the command as a Configuration
change. Followers don't propagate the configuration-change command to the user
state machine.


> **7** Once a given server adds the new configuration entry to its log, it uses
> that configuration for all future decisions (it does not wait for the entry to
> become committed).

A follower immediately applies a received configuration-change command. A
follower can't leverage the apply() functionality as a mechanism to apply
configuration changes.


> **8** This means that the leader will use the rules of C_old,new to determine
> when the log entry for C_old,new is committed.

A leader immediately applies a received configuration-change command.
Assuming it's not already in an unstable state (reject otherwise).


> **9** If the leader crashes, a new leader may be chosen under either C_old or
> C_old,new, depending on whether the winning candidate has received C_old,new.

_statement of fact_


> **10** In any case, C_new cannot make unilateral decisions during this period.

_statement of fact_


> **11** Once C_old,new has been committed, neither C_old nor C_new can make
> decisions without approval of the other, and the Leader Log Property ensures
> that only servers with the C_old,new log entry can be elected as leader.

_statement of fact_


> **12** It is now safe for the leader to create a log entry describing C_new
> and replicate it to the cluster.

Once the configuration-change command from C_old to C_old,new has been
committed, the leader transitions from C_old,new to C_new. The standard apply()
mechanism could be leveraged for this behavior.

Implementation: Configuration-change entry gets custom apply() that changes
leader Configuration.


> **13** Again, this configuration will take effect on each server as soon as it
> is seen.

_statement of fact_


> **14** When the new configuration has been committed under the rules of C_new,
> the old configuration is irrelevant and servers not in the new configuration
> can be shut down.

If a server receives a configuration-change command that results in a
configuration that doesn't include its server ID, when that command is
committed, it should shut itself down.

Implementation: if a configuration will result in the current server being
expelled and shut down, a `committed` channel is injected into the relevant
LogEntry, and a listener installed. If and when the LogEntry is committed, the
listener goroutine signals the server to shutdown.


> **15** As shown in Figure 9, there is no time when C_old and C_new can both
> make unilateral decisions; this guarantees safety.

_statement of fact_


> **16** There are two more issues to address for reconfiguration.

_statement of promise_


> **17** First, if the leader is part of C_old but not part of C_new, it must
> eventually step down.

_prefix of 18_


> **18** In Raft the leader steps down immediately after committing a
> configuration entry that does not include itself.

A leader that finds it's not a part of a new configuration change should step
down immediately after committing that change, but not before. The apply()
mechanism could be leveraged for this behavior. See **12**. Stepping down can be
synonymous with exiting its select loop -- the same behavior as followers.


> **19** This means that there will be a period of time (while it is committing
> C_new) where the leader is managing a cluster that does not include itself; it
> replicates log entries but does not count itself in majorities.

Leaders should take care to not automatically vote for themselves when
counting majorities for a [C_new] state that doesn't include them.

Implementation: when counting votes, range over the actual configuration
members, and compare against the passed vote-tally -- not the other way around.

> **20** The leader should not step down earlier, because members not in C_new
> could still be elected, resulting in unnecessary elections.

_statement of fact_


> **21** The second issue is that new servers may not initially store any log
> entries.

_statement of fact_


> **22** If they are added to the cluster in this state, it could take quite a
> while for them to catch up, during which time it might not be possible to
> commit new log entries.

_statement of fact_


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
