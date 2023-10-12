package chandy_lamport

import "log"

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	Snaps map[int]*serverSnapshot // key = snapshotId
}

// server's local snapshot state
type serverSnapshot struct {
	CLstarted   bool
	CLcompleted map[string]bool //size = len(outboundLinks)
	numtoken    int
	messages    []*SnapshotMessage
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		// TODO
		make(map[int]*serverSnapshot),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

func (server *Server) receiveTokens(src string, message TokenMessage) {
	server.Tokens += message.numTokens
	for _, snap := range server.Snaps {
		if snap.CLstarted && !snap.CLcompleted[src] {
			snap.messages = append(snap.messages, &SnapshotMessage{src, server.Id, message})
		}
	}
}

func (server *Server) receiveMarker(src string, message MarkerMessage) {
	id := message.snapshotId
	snap := server.Snaps[id]
	if snap == nil {
		server.StartSnapshot(id)
	} else {
		snap.CLcompleted[src] = true
		if len(snap.CLcompleted) == len(server.inboundLinks) {
			go server.sim.NotifySnapshotComplete(server.Id, id)
		}
	}
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	server.sim.logger.RecordEvent(server, ReceivedMessageEvent{src: src, dest: server.Id, message: message})
	switch message := message.(type) {
	case TokenMessage:
		server.receiveTokens(src, message)
	case MarkerMessage:
		server.receiveMarker(src, message)
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	snap := serverSnapshot{true, make(map[string]bool), server.Tokens, make([]*SnapshotMessage, 0)}
	server.Snaps[snapshotId] = &snap

	// send marker on all outgoing channel
	server.SendToNeighbors(MarkerMessage{snapshotId: snapshotId})

}
