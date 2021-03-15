package bareminpaxos

import (
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"math"
	"minpaxosproto"
	"net"
	"state"
	"time"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const MAX_BATCH = 5000

type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica
	prepareChan         chan fastrpc.Serializable
	acceptChan          chan fastrpc.Serializable
	commitChan          chan fastrpc.Serializable
	commitShortChan     chan fastrpc.Serializable
	prepareReplyChan    chan fastrpc.Serializable
	acceptReplyChan     chan fastrpc.Serializable
	// proposalTimerChan   chan fastrpc.Serializable
	prepareRPC      uint8
	acceptRPC       uint8
	commitRPC       uint8
	commitShortRPC  uint8
	prepareReplyRPC uint8
	acceptReplyRPC  uint8
	// configRPC           uint8
	Leader             int32                     // who does the replica think is the leader
	instanceSpace      []*minpaxosproto.Instance // the space of all instances (used and not yet used)
	crtInstance        int32                     // highest active instance number that this replica knows about
	defaultBallot      int32                     // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
	Shutdown           bool
	counter            int
	flush              bool
	committedUpTo      int32
	Heartbeat          bool // leader sends heartbeats
	prepareBookkeeping PrepareBookkeeping
}

// type InstanceStatus int32

// const (
// 	PREPARING InstanceStatus = iota
// 	PREPARED
// 	ACCEPTED
// 	COMMITTED
// )

// type Instance struct {
// 	ballot int32
// 	status InstanceStatus
// 	lb     *LeaderBookkeeping
// 	cmds   []state.Command
// }

// type LeaderBookkeeping struct {
// 	maxRecvBallot   int32
// 	acceptOKs       int32
// 	nacks           int32
// 	clientProposals []*genericsmr.Propose
// }

type PrepareBookkeeping struct {
	maxRecvBallot         int32
	prepareOKs            int32
	nacks                 int32
	peerCommits           []int32
	highestInstanceNumber int32
	cmds                  []state.Command
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, heartbeat bool, durable bool) *Replica {
	r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		// make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0,
		int32(0),
		make([]*minpaxosproto.Instance, 15*1024*1024),
		0,
		-1,
		false,
		0,
		true,
		-1,
		false,
		PrepareBookkeeping{-1, 0, 0, make([]int32, 3), -1, nil}}

	r.Heartbeat = heartbeat
	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(minpaxosproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(minpaxosproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(minpaxosproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(minpaxosproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(minpaxosproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(minpaxosproto.AcceptReply), r.acceptReplyChan)
	// r.configRPC = r.RegisterRPC(new(genericsmrproto.Config), r.configChan)

	go r.run()

	return r
}

// rough outline, need to flesh out more
func (r *Replica) getDataFromStableStore() {

	for {

		var b [12]byte
		var bs []byte
		bs = b[:12]

		_, err := r.StableStore.Read(bs)
		if err != nil {
			if err == io.EOF {
				return
			} else {
				log.Printf("error: %v")
				panic(err)
			}
		}

		ballot := int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
		status := minpaxosproto.InstanceStatus((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
		instNo := int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))

		command := make([]state.Command, 1)
		command[0].Unmarshal(r.StableStore)

		if ballot > r.defaultBallot {
			r.defaultBallot = ballot
		}

		if instNo > r.committedUpTo && status == minpaxosproto.COMMITTED {
			r.committedUpTo = instNo
		}

		r.instanceSpace[instNo] = &minpaxosproto.Instance{
			ballot,
			status,
			&minpaxosproto.LeaderBookkeeping{0, 0, 0, nil},
			command}
	}
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *minpaxosproto.Instance, instNo int32) {
	if !r.Durable {
		return
	}

	var b [12]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.Ballot))
	binary.LittleEndian.PutUint32(b[4:8], uint32(inst.Status))
	binary.LittleEndian.PutUint32(b[8:12], uint32(instNo))
	r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

//sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

func (r *Replica) slowClock() {
	for !r.Shutdown {
		time.Sleep(150 * 1e7) // 150 ms
		slowClockChan <- true
	}
}

func (r *Replica) checkWriter(q int32) {
	// fmt.Println("in the safe flush function")
	defer func() {
		if err := recover(); err != nil {
			log.Printf("discovered during flush that server %d is offline: %s\n", q, err)
			r.Alive[q] = false
		}
	}()
	r.PeerWriters[q].Flush()
	// fmt.Println("At bottom of safe flush function")
}

/* RPC to be called by master */

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	r.Leader = r.Id
	return nil
}

func (r *Replica) replyPrepare(replicaId int32, reply *minpaxosproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *minpaxosproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

/* ============= */

var slowClockChan chan bool
var clockChan chan bool

func (r *Replica) clock() {
	for !r.Shutdown {
		time.Sleep(1000 * 1000 * 5)
		clockChan <- true
	}
}

/* Main event processing loop */

func (r *Replica) run() {
	initialBoot := false
	fi, err := r.StableStore.Stat()
	if err != nil {
		fmt.Println("error accessing stablestore")
	}

	log.Printf("file size %v\n", fi.Size())
	if fi.Size() == 0 {
		r.ConnectToPeers()
		initialBoot = true
	}

	if fi.Size() != 0 {
		// do some sort of reconnection code that is different from initial connection
		// fmt.Println("booting up differently, reconnection code\n")
		r.getDataFromStableStore()
		fmt.Println("finished reading from storage")
		r.Listener, _ = net.Listen("tcp", r.PeerAddrList[r.Id])
		// r.ReconnectToPeers()
	}

	dlog.Println("Waiting for client connections")

	go r.WaitForConnections()

	if r.Exec {
		go r.executeCommands()
	}

	slowClockChan = make(chan bool, 1)
	go r.slowClock()

	clockChan = make(chan bool, 1)
	go r.clock()

	onOffProposeChan := r.ProposeChan

	// set initial leader as server 0 to initialize the protocol
	if initialBoot && r.Id == 0 {
		r.Leader = r.Id
		r.defaultBallot = r.makeUniqueBallot(0)
		r.bcastPrepare(r.defaultBallot)
	}

	for !r.Shutdown {

		select {

		case <-clockChan:
			//activate the new proposals channel
			onOffProposeChan = r.ProposeChan
			break

		case propose := <-onOffProposeChan:
			//got a Propose from a client
			dlog.Printf("Proposal with op %d\n", propose.Command.Op)
			r.handlePropose(propose)
			//deactivate the new proposals channel to prioritize the handling of protocol messages
			onOffProposeChan = nil
			break

		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*minpaxosproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for ballot %d\n", prepare.LeaderId, prepare.Ballot)
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*minpaxosproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*minpaxosproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
			commit := commitS.(*minpaxosproto.CommitShort)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*minpaxosproto.PrepareReply)
			//got a Prepare reply
			dlog.Printf("Received PrepareReply from replica %d, for ballot %d\n", prepareReply.Id, prepareReply.Ballot)
			r.handlePrepareReply(prepareReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*minpaxosproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break

			// case beacon := <-r.ProposalTimeout:
			// 	// dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
			// 	r.ReplyBeacon(beacon)
			// 	break

			// case beacon := <-r.BeaconChan:
			// 	// dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
			// 	r.ReplyBeacon(beacon)
			// 	break

			// case <-slowClockChan:
			// 	if r.Beacon {
			// 		for q := int32(0); q < int32(r.N); q++ {
			// 			if q == r.Id {
			// 				continue
			// 			}
			// 			r.checkWriter(q)
			// 			if !r.Alive[q] {
			// 				// do new code for re-setting up writers and readers
			// 			}

			// 			if r.Alive[q] {
			// 				r.SendBeacon(q)
			// 			}

			// 		}
			// 	}
			break
		}
	}
}

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	return (ballot << 4) | r.Id
}

func (r *Replica) updateCommittedUpTo() {
	for r.instanceSpace[r.committedUpTo+1] != nil &&
		r.instanceSpace[r.committedUpTo+1].Status == minpaxosproto.COMMITTED {
		r.committedUpTo++
	}
}

func (r *Replica) bcastPrepare(ballot int32) {
	log.Printf("in bcast prepare")
	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	var cmds []state.Command
	instNo := r.committedUpTo
	// if the server has already accepted a value for the next instance
	if r.committedUpTo != -1 && r.crtInstance > r.committedUpTo {
		log.Printf("currentinstance %v on the leader %v has already accepted a value\n", r.crtInstance, r.Id)
		cmds = r.instanceSpace[r.crtInstance].Cmds
		instNo = r.crtInstance
	}

	r.prepareBookkeeping = PrepareBookkeeping{ballot, 0, 0, make([]int32, 3), instNo, cmds}

	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()

	for i := 0; i <= 5; i++ {
		log.Printf("log of server %v, instance %v: %+v\n", r.Id, i, r.instanceSpace[i])
	}

	args := &minpaxosproto.Prepare{r.Id, ballot, r.committedUpTo}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id

	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			log.Printf("not alive, didnt' try to send to %v\n", q)
			continue
		}
		sent++
		ok := r.SendMsg(q, r.prepareRPC, args)
		if !ok {
			log.Printf("sendmsg to %v didn't go through, setting r.Alive to false\n", q)
			r.Alive[q] = false
		}
	}
}

var pa minpaxosproto.Accept

func (r *Replica) bcastAccept(instance int32, ballot int32, lastcommitted int32, command []state.Command, peercommits []int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()

	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = ballot
	pa.LastCommitted = lastcommitted
	pa.Command = command

	//args := &minpaxosproto.Accept{r.Id, instance, ballot, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id

	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			log.Printf("replica %v is not alive attempting to reconnect\n", q)
			// attempt to reconnect to replica
			r.ReconnectToPeer(q)
		}

		sent++
		log.Printf("Broadcasting accept from replica %v to %v\n", r.Id, q)

		// log.Println(peercommits[q])
		// log.Println(lastcommitted)

		var partiallog []minpaxosproto.Instance

		// partiallog[0] = minpaxosproto.Instance{ballot,
		// 	minpaxosproto.ACCEPTED,
		// 	&minpaxosproto.LeaderBookkeeping{0, 0, 0, nil},
		// 	command}

		// setting the log to not throw an error if the receiving server has 0 commits
		if peercommits[q] < 0 && r.crtInstance >= 0 {
			// log.Printf("peercommits of %v: %v\n", q, peercommits[q])
			for i := int32(0); i <= lastcommitted; i++ {
				partiallog = append(partiallog, *r.instanceSpace[i])
			}
			// if the leader does not have an empty log
		} else if r.crtInstance >= 0 {
			log.Printf("peer commits %v\n", peercommits[q])
			for i := int32(peercommits[q] + 1); i <= lastcommitted; i++ {
				partiallog = append(partiallog, *r.instanceSpace[i])
			}
		} else {
			partiallog = nil
		}
		if len(partiallog) > 0 {
			log.Printf("length of partial log: %v\n", len(partiallog))
		}
		pa.CatchUpLog = partiallog
		args := &pa
		ok := r.SendMsg(q, r.acceptRPC, args)
		if !ok {
			r.Alive[q] = false
		}
	}
}

func (r *Replica) sendAccept(toreplica int32, instance int32, ballot int32, lastcommitted int32, command []state.Command, peercommits []int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()

	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = ballot
	pa.LastCommitted = lastcommitted
	pa.Command = command

	var culog []minpaxosproto.Instance

	if r.crtInstance > 0 {
		for i := int32(peercommits[toreplica]); i <= lastcommitted; i++ {
			culog[i-peercommits[toreplica]] = *r.instanceSpace[i]
		}
		pa.CatchUpLog = culog
	} else {
		culog = nil
	}

	//args := &minpaxosproto.Accept{r.Id, instance, ballot, command}

	if !r.Alive[toreplica] {
		log.Printf("replica %v is not alive, attempting to reconnect\n", toreplica)
		r.ReconnectToPeer(toreplica)
	}

	log.Printf("directly sending accept from replica %v to %v\n", r.Id, toreplica)

	args := &pa
	ok := r.SendMsg(toreplica, r.acceptRPC, args)
	if !ok {
		r.Alive[toreplica] = false
	}
}

var pc minpaxosproto.Commit
var pcs minpaxosproto.CommitShort

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
	args := &pc
	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.Count = int32(len(command))
	argsShort := &pcs

	//args := &minpaxosproto.Commit{r.Id, instance, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id
	sent := 0

	for sent < n {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.commitShortRPC, argsShort)
	}
	if r.Thrifty && q != r.Id {
		for sent < r.N-1 {
			q = (q + 1) % int32(r.N)
			if q == r.Id {
				break
			}
			if !r.Alive[q] {
				continue
			}
			sent++
			r.SendMsg(q, r.commitRPC, args)
		}
	}
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	if r.Leader != r.Id || r.prepareBookkeeping.prepareOKs <= int32(r.N)>>1 {
		// log.Printf("HANDLEPROPOSE, leader %v, r.id %v, prepareoks %v\n", r.Leader, r.Id, r.prepareBookkeeping.prepareOKs)
		// set timer to start election/prepare if you don't receive an accept in x amount of time
		// either do this and have client resend to correct one, or just resend to correct one yourself
		preply := &genericsmrproto.ProposeReplyTS{FALSE, -1, state.NIL, 0}
		r.ReplyProposeTS(preply, propose.Reply)
		return
	}

	for r.instanceSpace[r.crtInstance] != nil {
		// log.Printf("%v, %v\n", r.crtInstance, r.instanceSpace[r.crtInstance])
		r.crtInstance++
	}

	instNo := r.crtInstance

	batchSize := len(r.ProposeChan) + 1

	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}

	dlog.Printf("Batched %d\n", batchSize)

	cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.Propose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose

	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	// this is for first instance, no prepares already
	// if r.defaultBallot == -1 {
	// 	r.instanceSpace[instNo] = &minpaxosproto.Instance{
	// 		cmds,
	// 		r.makeUniqueBallot(0),
	// 		PREPARING,
	// 		&LeaderBookkeeping{proposals, 0, 0, 0, 0}}
	// 	r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
	// 	dlog.Printf("Classic round for instance %d\n", instNo)
	// } else {
	// 	r.instanceSpace[instNo] = &minpaxosproto.Instance{
	// 		cmds,
	// 		r.defaultBallot,
	// 		PREPARED,
	// 		&LeaderBookkeeping{proposals, 0, 0, 0, 0}}

	// rebroadcast a value that has not been accepted yet, and the the client know to resend their message to you
	if r.committedUpTo < instNo-1 {
		// r.crtInstance--
		for i := (int32(math.Max(float64(0), float64(instNo-6)))); i <= instNo-1; i++ {
			log.Printf("log of server %v, instance %v: %+v\n", r.Id, i, r.instanceSpace[i])
		}
		log.Println(r.committedUpTo + 1)
		log.Println(instNo)
		K := r.instanceSpace[r.committedUpTo+1].Cmds
		fmt.Println(len(K))
		r.bcastAccept(r.committedUpTo+1, r.defaultBallot, r.committedUpTo, r.instanceSpace[r.committedUpTo+1].Cmds, r.prepareBookkeeping.peerCommits)
		preply := &genericsmrproto.ProposeReplyTS{FALSE, -1, state.NIL, 0}
		r.ReplyProposeTS(preply, propose.Reply)
		return
	}

	if r.Leader == r.Id {
		r.crtInstance++
		if r.prepareBookkeeping.prepareOKs > int32(r.N)>>1 {
			// need to check for other values from prepare
			r.instanceSpace[instNo] = &minpaxosproto.Instance{
				r.defaultBallot,
				minpaxosproto.PREPARED,
				&minpaxosproto.LeaderBookkeeping{0, 0, 0, nil},
				cmds}

			r.recordInstanceMetadata(r.instanceSpace[instNo], instNo)
			r.recordCommands(cmds)
			r.sync()

			// this is sketchy
			r.bcastAccept(instNo, r.defaultBallot, r.committedUpTo, cmds, r.prepareBookkeeping.peerCommits)
			dlog.Printf("Fast round for instance %d\n", instNo)
		}

	} else { // not leader, take care of sending to right leader if I decide to, otherwise just send
		// to client telling them the correct leader
		dlog.Printf("Forwarding proposal to leader %v for instance %d\n", instNo)
	}
}

func (r *Replica) handlePrepare(prepare *minpaxosproto.Prepare) {
	var preply *minpaxosproto.PrepareReply
	var recentAccept []state.Command

	ok := FALSE

	if r.defaultBallot < prepare.Ballot {
		r.prepareBookkeeping = PrepareBookkeeping{prepare.Ballot, 0, 0, make([]int32, 3), 0, nil}
		ok = TRUE
		r.defaultBallot = prepare.Ballot
		r.Leader = prepare.LeaderId
	}

	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	dlog.Printf("in handle prepare, last instance is %v on server %v\n", r.crtInstance, r.Id)

	if r.committedUpTo <= prepare.LastCommitted {
		preply = &minpaxosproto.PrepareReply{r.Id, r.crtInstance - 1, ok, r.defaultBallot, r.committedUpTo, nil, nil}
	} else {
		// will need to catch errors/check here if this doesn't exist
		// get the most updated instance
		recentAccept = nil
		if r.crtInstance > r.committedUpTo {
			temp := *r.instanceSpace[r.committedUpTo+1]
			recentAccept = temp.Cmds
		}

		var culog []minpaxosproto.Instance
		for i := int32(prepare.LastCommitted + 1); i <= r.committedUpTo; i++ {
			culog[i-(prepare.LastCommitted+1)] = *r.instanceSpace[i]
		}

		preply = &minpaxosproto.PrepareReply{r.Id, r.crtInstance - 1, ok, r.defaultBallot, r.committedUpTo, recentAccept, culog}
	}

	r.replyPrepare(prepare.LeaderId, preply)
}

func (r *Replica) handleAccept(accept *minpaxosproto.Accept) {
	var areply *minpaxosproto.AcceptReply

	// need to be able to check if we already accepted this same message previously and not send an ok.
	if r.instanceSpace[accept.Instance] != nil {
		if r.instanceSpace[accept.Instance].Status == minpaxosproto.ACCEPTED && r.instanceSpace[accept.Instance].Ballot == accept.Ballot {
			log.Printf("in handle accept, ignoring a resent accept from leader\n")
			return
		}
	}

	log.Printf("in handle accept, committedUpTo is %v, accept committed up to is %v\n", r.committedUpTo, accept.LastCommitted)

	// update committed from piggyback commit and/or update many that you were missing during prepare
	if r.committedUpTo < accept.LastCommitted && len(accept.CatchUpLog) != 0 {
		for i := r.committedUpTo + 1; i <= accept.LastCommitted; i++ {
			r.instanceSpace[i] = &accept.CatchUpLog[i-(r.committedUpTo+1)]
			r.recordInstanceMetadata(r.instanceSpace[i], i)
			r.recordCommands(r.instanceSpace[i].Cmds)
			r.sync()
		}

		r.committedUpTo = accept.LastCommitted
	}

	// treat accept from far in the future as a prepare
	if r.defaultBallot < accept.Ballot || (r.committedUpTo+1) < accept.LastCommitted {
		r.defaultBallot = accept.Ballot
		preply := &minpaxosproto.PrepareReply{r.Id, r.committedUpTo + 1, TRUE, r.defaultBallot, r.committedUpTo, nil, nil}
		r.replyPrepare(accept.LeaderId, preply)

	} else if r.defaultBallot == accept.Ballot {
		inst := &minpaxosproto.Instance{
			accept.Ballot,
			minpaxosproto.ACCEPTED,
			&minpaxosproto.LeaderBookkeeping{0, 0, 0, nil},
			accept.Command}

		r.instanceSpace[accept.Instance] = inst
		areply = &minpaxosproto.AcceptReply{accept.Instance, TRUE, accept.Ballot, r.Id}

		r.replyAccept(accept.LeaderId, areply)

		r.recordInstanceMetadata(r.instanceSpace[accept.Instance], accept.Instance)
		r.recordCommands(accept.Command)
		r.sync()
	}

	for i := (int32(math.Max(float64(0), float64(accept.Instance-5)))); i <= accept.Instance; i++ {
		log.Printf("log of server %v, instance %v: %+v\n", r.Id, i, r.instanceSpace[i])
	}
}

// old stuff
// if inst == nil {
// 	// log.Printf("inst = nil")
// 	if accept.Ballot < r.defaultBallot {
// 		areply = &minpaxosproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot}
// 	} else {
// 		r.instanceSpace[accept.Instance] = &minpaxosproto.Instance{
// 			accept.Command,
// 			accept.Ballot,
// 			ACCEPTED,
// 			nil}
// 		areply = &minpaxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
// 	}
// } else if inst.ballot > accept.Ballot {
// 	// log.Printf("inst ballot > accept ballot")
// 	areply = &minpaxosproto.AcceptReply{accept.Instance, FALSE, inst.ballot}
// } else if inst.ballot < accept.Ballot {
// 	// log.Printf("inst ballot < accept ballot")
// 	inst.Cmds = accept.Command
// 	inst.ballot = accept.Ballot
// 	inst.status = ACCEPTED
// 	areply = &minpaxosproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
// 	if inst.lb != nil && inst.Lb.ClientProposals != nil {
// 		//TODO: is this correct?
// 		// try the proposal in a different instance
// 		for i := 0; i < len(inst.Lb.ClientProposals); i++ {
// 			r.ProposeChan <- inst.Lb.ClientProposals[i]
// 		}
// 		inst.Lb.ClientProposals = nil
// 	}
// } else {
// 	// log.Printf("inst != nil, inst ballow !<, !> accept ballot")
// 	// reordered ACCEPT
// 	r.instanceSpace[accept.Instance].cmds = accept.Command
// 	if r.instanceSpace[accept.Instance].status != COMMITTED {
// 		r.instanceSpace[accept.Instance].status = ACCEPTED
// 	}
// 	areply = &minpaxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
// }

// if areply.OK == TRUE {
// 	r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
// 	r.recordCommands(accept.Command)
// 	r.sync()
// }

// r.replyAccept(accept.LeaderId, areply)

func (r *Replica) handleCommit(commit *minpaxosproto.Commit) {
	inst := r.instanceSpace[commit.Instance]

	dlog.Printf("Committing instance %d\n", commit.Instance)

	if inst == nil {
		r.instanceSpace[commit.Instance] = &minpaxosproto.Instance{
			commit.Ballot,
			minpaxosproto.COMMITTED,
			nil,
			commit.Command}
	} else {
		r.instanceSpace[commit.Instance].Cmds = commit.Command
		r.instanceSpace[commit.Instance].Status = minpaxosproto.COMMITTED
		r.instanceSpace[commit.Instance].Ballot = commit.Ballot
		if inst.Lb != nil && inst.Lb.ClientProposals != nil {
			for i := 0; i < len(inst.Lb.ClientProposals); i++ {
				r.ProposeChan <- inst.Lb.ClientProposals[i]
			}
			inst.Lb.ClientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance], commit.Instance)
	r.recordCommands(commit.Command)
}

func (r *Replica) handleCommitShort(commit *minpaxosproto.CommitShort) {
	inst := r.instanceSpace[commit.Instance]

	dlog.Printf("Committing instance %d\n", commit.Instance)

	if inst == nil {
		r.instanceSpace[commit.Instance] = &minpaxosproto.Instance{
			commit.Ballot,
			minpaxosproto.COMMITTED,
			nil,
			nil}
	} else {
		r.instanceSpace[commit.Instance].Status = minpaxosproto.COMMITTED
		r.instanceSpace[commit.Instance].Ballot = commit.Ballot
		if inst.Lb != nil && inst.Lb.ClientProposals != nil {
			for i := 0; i < len(inst.Lb.ClientProposals); i++ {
				r.ProposeChan <- inst.Lb.ClientProposals[i]
			}
			inst.Lb.ClientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance], commit.Instance)
}

func (r *Replica) handlePrepareReply(preply *minpaxosproto.PrepareReply) {
	log.Printf("default ballot %v, preply ballot %v\n", r.defaultBallot, preply.Ballot)
	log.Println(preply)
	// might still want to send something... not sure
	if r.defaultBallot > preply.Ballot {
		return
	}

	// log catch up / prepare to catch up other logs
	if r.defaultBallot == preply.Ballot {
		r.prepareBookkeeping.prepareOKs++
		// do this if so leader can learn of accepted values for the most recent instances
		if preply.Instance > r.prepareBookkeeping.highestInstanceNumber || (preply.Instance == r.prepareBookkeeping.highestInstanceNumber && preply.Ballot > r.prepareBookkeeping.maxRecvBallot) {
			// need to check if there is a command sent
			r.prepareBookkeeping.cmds = preply.Command
			r.prepareBookkeeping.maxRecvBallot = preply.Ballot
			log.Printf("in preparereply, last commit from server %v was %v\n", preply.Id, preply.LastCommitted)
			r.prepareBookkeeping.peerCommits[preply.Id] = preply.LastCommitted
			r.prepareBookkeeping.highestInstanceNumber = preply.Instance
		}

		// leader updates log from responses
		if r.committedUpTo <= preply.LastCommitted {
			// update our own log
			for i := r.committedUpTo + 1; i <= preply.LastCommitted; i++ {
				r.instanceSpace[i] = &preply.CatchUpLog[i-(r.committedUpTo+1)]
			}
			r.committedUpTo = preply.LastCommitted
		}

		// if there's a new value in a new instance that we've heard from other replicas, send accept for this value
		log.Printf("highest instance number from preplies: %v\n", r.prepareBookkeeping.highestInstanceNumber)
		log.Printf("committed up to on leader: %v\n", r.committedUpTo)
		if r.prepareBookkeeping.prepareOKs == int32(r.N)>>1 && r.prepareBookkeeping.highestInstanceNumber > r.committedUpTo {
			// check if there are any accept, if no accepts, use the instance after your last commit

			r.instanceSpace[r.prepareBookkeeping.highestInstanceNumber] = &minpaxosproto.Instance{
				r.defaultBallot,
				minpaxosproto.ACCEPTED,
				&minpaxosproto.LeaderBookkeeping{0, 0, 0, nil},
				r.prepareBookkeeping.cmds}

			r.committedUpTo = r.prepareBookkeeping.highestInstanceNumber

			r.recordInstanceMetadata(r.instanceSpace[preply.Instance], preply.Instance)
			r.sync()
			r.bcastAccept(r.prepareBookkeeping.highestInstanceNumber, r.defaultBallot, r.committedUpTo, r.prepareBookkeeping.cmds, r.prepareBookkeeping.peerCommits) // use peercommits to calc what logs to send
		}
	}

	// maybe don't send any accepts unless theres a special preparereply flag from the future accept
	// if r.prepareBookkeeping.prepareOKs > int32(r.N)>>1 {
	// 	r.sendAccept(preply.Id, r.prepareBookkeeping.highestInstanceNumber, r.defaultBallot, r.committedUpTo, r.prepareBookkeeping.cmds, r.prepareBookkeeping.peerCommits)
	// }
}

// // old stuff

// if preply.OK == TRUE {
// 	inst.lb.prepareOKs++

// 	if preply.Ballot > inst.lb.maxRecvBallot {
// 		inst.Cmds = preply.Command
// 		inst.lb.maxRecvBallot = preply.Ballot
// 		if inst.Lb.ClientProposals != nil {
// 			// there is already a competing command for this instance,
// 			// so we put the client proposal back in the queue so that
// 			// we know to try it in another instance
// 			for i := 0; i < len(inst.Lb.ClientProposals); i++ {
// 				r.ProposeChan <- inst.Lb.ClientProposals[i]
// 			}
// 			inst.Lb.ClientProposals = nil
// 		}
// 	}

// 	if inst.lb.prepareOKs+1 > r.N>>1 {
// 		inst.status = PREPARED
// 		inst.lb.nacks = 0
// 		if inst.ballot > r.defaultBallot {
// 			r.defaultBallot = inst.ballot
// 		}
// 		r.recordInstanceMetadata(r.instanceSpace[preply.Instance])
// 		r.sync()
// 		r.bcastAccept(preply.Instance, inst.ballot, inst.Cmds)
// 	}
// } else {
// 	// TODO: there is probably another active leader
// 	inst.lb.nacks++
// 	if preply.Ballot > inst.lb.maxRecvBallot {
// 		inst.lb.maxRecvBallot = preply.Ballot
// 	}
// 	if inst.lb.nacks >= r.N>>1 {
// 		if inst.Lb.ClientProposals != nil {
// 			// try the proposals in another instance
// 			for i := 0; i < len(inst.Lb.ClientProposals); i++ {
// 				r.ProposeChan <- inst.Lb.ClientProposals[i]
// 			}
// 			inst.Lb.ClientProposals = nil
// 		}
// 	}
// }

func (r *Replica) handleAcceptReply(areply *minpaxosproto.AcceptReply) {
	inst := r.instanceSpace[areply.Instance]

	// will probably want something like this
	// if inst.status != PREPARED && inst.status != ACCEPTED {
	// 	// we've move on, these are delayed replies, so just ignore
	// 	return
	// }

	if areply.OK == TRUE {
		inst.Lb.AcceptOKs++
		if inst.Lb.AcceptOKs+1 > int32(r.N)>>1 {
			log.Printf("instance %v committed on leader %v\n", areply.Instance, r.Id)
			inst.Status = minpaxosproto.COMMITTED
			if inst.Lb.ClientProposals != nil && !r.Dreply {
				// give client the all clear
				for i := 0; i < len(inst.Cmds); i++ {
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						inst.Lb.ClientProposals[i].CommandId,
						state.NIL,
						inst.Lb.ClientProposals[i].Timestamp}
					r.ReplyProposeTS(propreply, inst.Lb.ClientProposals[i].Reply)
				}
			}

			r.recordInstanceMetadata(r.instanceSpace[areply.Instance], areply.Instance)
			r.sync() //is this necessary?

			r.committedUpTo = areply.Instance
			r.prepareBookkeeping.peerCommits[areply.Id] = areply.Instance - 1
			// log.Printf("PEER COMMITS FOR %v, %v\n", areply.Id, areply.Instance-1)
			// need to update peer commits here. might need to change accept reply structure.
		}
		// } else {
		// 	// TODO: there is probably another active leader
		// 	inst.lb.nacks++
		// 	if areply.Ballot > inst.lb.maxRecvBallot {
		// 		inst.lb.maxRecvBallot = areply.Ballot
		// 	}
		// 	if inst.lb.nacks >= r.N>>1 {
		// 		// TODO
		// 	}
	}
}

func (r *Replica) executeCommands() {
	i := int32(0)
	for !r.Shutdown {
		executed := false

		for i <= r.committedUpTo {
			if r.instanceSpace[i].Cmds != nil {
				inst := r.instanceSpace[i]
				for j := 0; j < len(inst.Cmds); j++ {
					val := inst.Cmds[j].Execute(r.State)
					if r.Dreply && inst.Lb != nil && inst.Lb.ClientProposals != nil {
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							inst.Lb.ClientProposals[j].CommandId,
							val,
							inst.Lb.ClientProposals[j].Timestamp}
						r.ReplyProposeTS(propreply, inst.Lb.ClientProposals[j].Reply)
					}
				}
				i++
				executed = true
			} else {
				break
			}
		}

		if !executed {
			time.Sleep(1000 * 1000)
		}
	}

}
