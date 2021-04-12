package genericsmr

import (
	"bufio"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmrproto"
	"io"
	"log"
	"net"
	"os"
	"rdtsc"
	"state"
	"time"
)

const CHAN_BUFFER_SIZE = 200000

type RPCPair struct {
	Obj  fastrpc.Serializable
	Chan chan fastrpc.Serializable
}

type Propose struct {
	*genericsmrproto.Propose
	Reply *bufio.Writer
}

type Beacon struct {
	Rid       int32
	Timestamp uint64
}

type Replica struct {
	N            int        // total number of replicas
	Id           int32      // the ID of the current replica
	PeerAddrList []string   // array with the IP:port address of every replica
	Peers        []net.Conn // cache of connections to all other replicas
	PeerReaders  []*bufio.Reader
	PeerWriters  []*bufio.Writer
	Alive        []bool // connection status
	Listener     net.Listener

	State *state.State

	ProposeChan chan *Propose // channel for client proposals
	BeaconChan  chan *Beacon  // channel for beacons from peer replicas

	Shutdown bool

	Thrifty bool // send only as many messages as strictly required?
	Exec    bool // execute commands?
	Dreply  bool // reply to client after command has been executed?
	Beacon  bool // send beacons to detect how fast are the other replicas?

	Durable     bool     // log to a stable store?
	StableStore *os.File // file support for the persistent log

	PreferredPeerOrder []int32 // replicas in the preferred order of communication

	rpcTable map[uint8]*RPCPair
	rpcCode  uint8

	Ewma []float64

	OnClientConnect chan bool
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool) *Replica {
	r := &Replica{
		len(peerAddrList),
		int32(id),
		peerAddrList,
		make([]net.Conn, len(peerAddrList)),
		make([]*bufio.Reader, len(peerAddrList)),
		make([]*bufio.Writer, len(peerAddrList)),
		make([]bool, len(peerAddrList)),
		nil,
		state.InitState(),
		make(chan *Propose, CHAN_BUFFER_SIZE),
		make(chan *Beacon, CHAN_BUFFER_SIZE),
		false,
		thrifty,
		exec,
		dreply,
		false,
		false,
		nil,
		make([]int32, len(peerAddrList)),
		make(map[uint8]*RPCPair),
		genericsmrproto.GENERIC_SMR_BEACON_REPLY + 1,
		make([]float64, len(peerAddrList)),
		make(chan bool, 100)}

	var err error

	// open stablestore if already exists, create if not
	if r.StableStore, err = os.Open(fmt.Sprintf("stable-store-replica%d", r.Id)); err != nil {
		if r.StableStore, err = os.Create(fmt.Sprintf("stable-store-replica%d", r.Id)); err != nil {
			log.Fatal(err)
		}
	}

	for i := 0; i < r.N; i++ {
		r.PreferredPeerOrder[i] = int32((int(r.Id) + 1 + i) % r.N)
		r.Ewma[i] = 0.0
	}

	return r
}

/* Client API */

func (r *Replica) Ping(args *genericsmrproto.PingArgs, reply *genericsmrproto.PingReply) error {
	return nil
}

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	return nil
}

/* ============= */

func (r *Replica) ConnectToPeers() {
	var b [4]byte
	bs := b[:4]
	connbs := make([]byte, 1)
	connbs[0] = byte(genericsmrproto.PEER)

	done := make(chan bool)

	go r.waitForPeerConnections(done)

	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		log.Printf("Server %v is trying to connect to server %v\n", r.Id, i)
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				log.Printf("Successful connection from server %v to server %v\n", r.Id, i)
				done = true
			} else {
				fmt.Printf("encountered an error %v when connecting %v to peer %v \n", err, r.Id, i)
				time.Sleep(1e9)
			}
		}

		if _, err := r.Peers[i].Write(connbs); err != nil {
			fmt.Println("Write connection type error:", err)
			continue
		}

		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)

	for rid, reader := range r.PeerReaders {
		if int32(rid) == r.Id {
			continue
		}
		go r.replicaListener(rid, reader)
	}
}

func (r *Replica) ConnectToPeersNoListeners() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)
}

func (r *Replica) ReconnectToPeers() {
	var b [4]byte
	bs := b[:4]
	connbs := make([]byte, 1)
	connbs[0] = byte(genericsmrproto.PEER)

	//connect to peers
	for i := 0; i < r.N; i++ {
		if i == int(r.Id) {
			continue
		}

		log.Printf("Server %v is trying to reconnect to server %v\n", r.Id, i)
		if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
			r.Peers[i] = conn
			log.Printf("Successful reconnection from server %v to server %v\n", r.Id, i)
		} else {
			fmt.Printf("encountered an error %v when reconnecting %v to peer %v \n", err, r.Id, i)
			time.Sleep(1e9)
		}

		if _, err := r.Peers[i].Write(connbs); err != nil {
			fmt.Println("Write connection type error:", err)
			continue
		}

		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}

	log.Printf("Replica id: %d. Finished reconnection loop\n", r.Id)

	for rid, reader := range r.PeerReaders {
		if int32(rid) == r.Id {
			continue
		}
		if r.Alive[rid] == false {
			log.Printf("couldn't connect to server %v\n", rid)
			continue
		}
		go r.replicaListener(rid, reader)
	}
}

func (r *Replica) ReconnectToPeer(q int32) {
	var b [4]byte
	bs := b[:4]
	connbs := make([]byte, 1)
	connbs[0] = byte(genericsmrproto.PEER)

	//connect to peers

	log.Printf("Server %v is trying to reconnect to server %v\n", r.Id, q)
	if conn, err := net.Dial("tcp", r.PeerAddrList[q]); err == nil {
		r.Peers[q] = conn
		log.Printf("Successful reconnection from server %v to server %v\n", r.Id, q)
	} else {
		fmt.Printf("encountered an error %v when reconnecting %v to peer %v \n", err, r.Id, q)
		time.Sleep(1e9)
	}

	if _, err := r.Peers[q].Write(connbs); err != nil {
		fmt.Println("Write connection type error:", err)
	}

	binary.LittleEndian.PutUint32(bs, uint32(r.Id))
	if _, err := r.Peers[q].Write(bs); err != nil {
		fmt.Println("Write id error:", err)
	}

	r.Alive[q] = true
	r.PeerReaders[q] = bufio.NewReader(r.Peers[q])
	r.PeerWriters[q] = bufio.NewWriter(r.Peers[q])

	log.Printf("Replica id: %d. Finished reconnection loop\n", r.Id)

	go r.replicaListener(int(q), r.PeerReaders[q])
}

/* Peer (replica) connections dispatcher */
func (r *Replica) waitForPeerConnections(done chan bool) {
	var b [5]byte
	bs := b[:5]

	r.Listener, _ = net.Listen("tcp", r.PeerAddrList[r.Id])
	for i := r.Id + 1; i < int32(r.N); i++ {
		conn, err := r.Listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			fmt.Printf("Connection establish error: %v. trying to close connection on %v to %v\n", err, r.Id, i)
			err := conn.Close()
			conn, err := r.Listener.Accept()
			log.Println("GOT PAST ACCEPT")
			if err != nil {
				fmt.Println("Accept error:", err)
				continue
			}
			if _, err := io.ReadFull(conn, bs); err != nil {
				fmt.Println("Connection establish error:", err)
				continue
			}
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs[1:5]))
		fmt.Printf("Finished the connection process from %v to %v\n", r.Id, id)
		r.Peers[id] = conn
		r.PeerReaders[id] = bufio.NewReader(conn)
		r.PeerWriters[id] = bufio.NewWriter(conn)
		r.Alive[id] = true
	}
	done <- true
}

/* Client connections dispatcher */
func (r *Replica) WaitForClientConnections() {
	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		go r.clientListener(conn)

		r.OnClientConnect <- true
	}
}

/* Client connections dispatcher */
func (r *Replica) WaitForConnections() {
	var connType uint8

	for !r.Shutdown {
		fmt.Printf("entering wait for connections for server %v\n", r.Id)
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		fmt.Printf("after accept connections for server %v\n", r.Id)

		reader := bufio.NewReader(conn)

		if connType, err = reader.ReadByte(); err != nil {
			fmt.Printf("error %v", err)
			break
		}

		log.Printf("received on server %v. types: client %v, peer %v, conntype %v\n", r.Id, genericsmrproto.CLIENT, genericsmrproto.PEER, connType)
		switch uint8(connType) {

		case genericsmrproto.CLIENT:
			go r.clientListener(conn)
			r.OnClientConnect <- true

		case genericsmrproto.PEER:
			go r.peerReconnector(conn, reader)

		default:
			log.Printf("Error: received unknown connection type %v\n", connType)
		}
	}
}

/* Client connections dispatcher */
func (r *Replica) peerReconnector(conn net.Conn, reader *bufio.Reader) {
	var b [4]byte
	bs := b[:4]
	var err error = nil

	// log.Printf("peer message received on server %v\n", r.Id)
	if bs, err = reader.Peek(4); err != nil {
		log.Println("Error reading id from peer on reconnect:", err)
	}

	id := int32(binary.LittleEndian.Uint32(bs))
	fmt.Println(id)

	// discard the peeked bytes
	reader.Discard(4)

	fmt.Printf("Finished the reconnection process from %v to %v\n", r.Id, id)
	r.Peers[id] = conn
	r.PeerReaders[id] = bufio.NewReader(conn)
	r.PeerWriters[id] = bufio.NewWriter(conn)
	r.Alive[id] = true

	go r.replicaListener(int(id), r.PeerReaders[id])
}

func (r *Replica) replicaListener(rid int, reader *bufio.Reader) {
	var msgType uint8
	var err error = nil
	var gbeacon genericsmrproto.Beacon
	var gbeaconReply genericsmrproto.BeaconReply

	for err == nil && !r.Shutdown {

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case genericsmrproto.GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			beacon := &Beacon{int32(rid), gbeacon.Timestamp}
			r.BeaconChan <- beacon
			break

		case genericsmrproto.GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			//TODO: UPDATE STUFF
			r.Ewma[rid] = 0.99*r.Ewma[rid] + 0.01*float64(rdtsc.Cputicks()-gbeaconReply.Timestamp)
			// log.Println(r.Ewma)
			break

		default:
			if rpair, present := r.rpcTable[msgType]; present {
				obj := rpair.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				rpair.Chan <- obj
			} else {
				log.Println("Error: received unknown message type")
			}
		}
	}
	log.Println("exiting replica")
}

func (r *Replica) clientListener(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	var msgType byte //:= make([]byte, 1)
	var err error
	for !r.Shutdown && err == nil {

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case genericsmrproto.PROPOSE:
			prop := new(genericsmrproto.Propose)
			if err = prop.Unmarshal(reader); err != nil {
				break
			}
			// fmt.Println("sending propose")
			r.ProposeChan <- &Propose{prop, writer}
			break

		case genericsmrproto.READ:
			read := new(genericsmrproto.Read)
			if err = read.Unmarshal(reader); err != nil {
				break
			}
			//r.ReadChan <- read
			break

		case genericsmrproto.PROPOSE_AND_READ:
			pr := new(genericsmrproto.ProposeAndRead)
			if err = pr.Unmarshal(reader); err != nil {
				break
			}
			//r.ProposeAndReadChan <- pr
			break
		}
	}
	if err != nil && err != io.EOF {
		log.Println("Error when reading from client connection:", err)
	}
}

func (r *Replica) RegisterRPC(msgObj fastrpc.Serializable, notify chan fastrpc.Serializable) uint8 {
	code := r.rpcCode
	r.rpcCode++
	r.rpcTable[code] = &RPCPair{msgObj, notify}
	return code
}

func (r *Replica) SendMsg(peerId int32, code uint8, msg fastrpc.Serializable) bool {
	ok := true
	w := r.PeerWriters[peerId]
	// log.Printf("writer for %v, %v\n", peerId, w)
	w.WriteByte(code)
	msg.Marshal(w)
	err := w.Flush()
	if err != nil {
		log.Printf("ERROR %v\n", err)
		ok = false
	}
	// log.Printf("FLUSHING from %v TO %v\n", r.Id, peerId)
	return ok
}

func (r *Replica) SendMsgNoFlush(peerId int32, code uint8, msg fastrpc.Serializable) {
	w := r.PeerWriters[peerId]
	w.WriteByte(code)
	msg.Marshal(w)
}

func (r *Replica) ReplyPropose(reply *genericsmrproto.ProposeReply, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(genericsmrproto.PROPOSE_REPLY)
	w.Flush()
	reply.Marshal(w)
	w.Flush()
}

func (r *Replica) ReplyProposeTS(reply *genericsmrproto.ProposeReplyTS, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(genericsmrproto.PROPOSE_REPLY)
	reply.Marshal(w)
	w.Flush()
}

func (r *Replica) SendBeacon(peerId int32) {
	w := r.PeerWriters[peerId]
	w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON)
	beacon := &genericsmrproto.Beacon{rdtsc.Cputicks()}
	beacon.Marshal(w)
	w.Flush()
}

func (r *Replica) ReplyBeacon(beacon *Beacon) {
	w := r.PeerWriters[beacon.Rid]
	w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON_REPLY)
	rb := &genericsmrproto.BeaconReply{beacon.Timestamp}
	rb.Marshal(w)
	w.Flush()
}

// updates the preferred order in which to communicate with peers according to a preferred quorum
func (r *Replica) UpdatePreferredPeerOrder(quorum []int32) {
	aux := make([]int32, r.N)
	i := 0
	for _, p := range quorum {
		if p == r.Id {
			continue
		}
		aux[i] = p
		i++
	}

	for _, p := range r.PreferredPeerOrder {
		found := false
		for j := 0; j < i; j++ {
			if aux[j] == p {
				found = true
				break
			}
		}
		if !found {
			aux[i] = p
			i++
		}
	}

	r.PreferredPeerOrder = aux
}
