package minpaxosproto

import (
	"genericsmr"
	"state"
)

type InstanceStatus int32

const (
	PREPARING InstanceStatus = iota
	PREPARED
	ACCEPTED
	COMMITTED
)

type Instance struct {
	Ballot int32
	Status InstanceStatus
	Lb     *LeaderBookkeeping
	Cmds   []state.Command
}

type LeaderBookkeeping struct {
	MaxRecvBallot   int32
	AcceptOKs       int32
	Nacks           int32
	ClientProposals []*genericsmr.Propose
}

const (
	PREPARE uint8 = iota
	PREPARE_REPLY
	ACCEPT
	ACCEPT_REPLY
	COMMIT
	COMMIT_SHORT
)

type Prepare struct {
	LeaderId int32
	// Instance   int32
	Ballot int32
	// ToInfinity uint8
	LastCommitted int32
}

type PrepareReply struct {
	Id            int32
	Instance      int32 //next instance after last committed
	OK            uint8
	Ballot        int32
	LastCommitted int32
	Command       []state.Command // the value you've accept already, if any
	CatchUpLog    []Instance
}

type Accept struct {
	LeaderId      int32
	Instance      int32
	Ballot        int32
	LastCommitted int32
	Command       []state.Command
	CatchUpLog    []Instance
}

type AcceptReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
	Id       int32
}

type Commit struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []state.Command
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	Count    int32
	Ballot   int32
}
