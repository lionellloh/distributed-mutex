package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"time"
)

//Code for implementing Distributed Mutual Exclusion through Lamport's Shared Priority Queue

//INSTRUCTIONS:

/*

Using the GO language, implement the following distributed mutual exclusion protocols using at least ten (10) nodes:
1. (10 marks) Lamport’s shared timestamp queue without Ricart and Agrawala’s optimization.
2. (10 marks) Lamport’s shared timestamp queue with Ricart and Agrawala’s optimization.
3. (5 marks) Centralized server protocol.

(15 marks) Compare the performance of the three implementations (in terms of time). In particular, increase the
number of nodes simultaneously requesting to enter the critical sections to investigate the performance trend for
each of the protocol. For each experiment and for each protocol, compute the time between the first request and all
the requesters exit the critical section. If you have ten nodes, therefore, your performance table should contain a
total of 30 entries (ten entries for each of the three protocols).
*/

type Node struct {
	id           int
	logicalClock int
	nodeChannel  chan Message
	pq           []Message
	ptrMap       map[int]*Node
	//A map to track the replies the node has received for its own request stamped with timestamp t
	//{requestTimeStamp: {nodeId: True} etc}
	replyTracker map[int]map[int]bool
	//requestTimeStamp: {nodeId: []Message}
	pendingReplies map[int][]Message
}

type MessageType string

const (
	Request MessageType = "Request"
	Reply   MessageType = "Reply"
	Release MessageType = "Release"
)

type Message struct {
	messageType MessageType
	message     string
	senderID    int
	timestamp   int
	//[id, timestamp]
	replyTarget ReplyTarget
}

type ReplyTarget struct {
	targetID  int
	timestamp int
}

//Constructor functions

func NewNode(id int) *Node {
	channel := make(chan Message)
	var pq []Message
	//Create a blank map to track
	var replyTracker = map[int]map[int]bool{}
	n := Node{id, 0, channel, pq, nil, replyTracker, map[int][]Message{}}

	return &n
}

func NewMessage(messageType MessageType, message string, senderID int, priority int) *Message {
	rt := ReplyTarget{-1, -1}
	m := Message{messageType, message, senderID, priority, rt}

	return &m
}

//Struct Methods
func (n *Node) setPtrMap(ptrMap map[int]*Node) {
	n.ptrMap = ptrMap
}

func (n *Node) enqueue(newMsg Message) {
	//TODO: Write the logic for inserting a new message in the correct position
	var insertPos int
	var foundPos bool
	for i, m := range(n.pq) {
		if newMsg.timestamp < m.timestamp {
			insertPos = i
			foundPos = true
		} else if newMsg.timestamp == m.timestamp && newMsg.senderID < m.senderID {
			insertPos = i
			foundPos = true
		}
	}

	if ! foundPos {
		insertPos = len(n.pq)
	}

	fmt.Println("BEFORE: ", printPQ(n.pq))
	n.pq = append(n.pq, Message{
		messageType: "",
		message:     "",
		senderID:    -1,
		timestamp:   -1,
		replyTarget: ReplyTarget{},
	})


	copy(n.pq[insertPos + 1: ], n.pq[insertPos : ])
	n.pq[insertPos] = newMsg
	fmt.Println("AFTER: ", printPQ(n.pq))

}

//TODO: request to enter CS
//TODO: How to simulate a node's need to enter the CS?
func (n *Node) requestCS() {
	fmt.Printf("=======================================\n Node %d is " +
		"requesting to enter the CS \n =======================================\n", n.id)
	time.Sleep(time.Duration(500) * time.Millisecond)
	n.logicalClock += 1
	requestMsg := Message{
		messageType: "Request",
		message:     "",
		senderID:    n.id,
		timestamp:   n.logicalClock,
	}
	n.enqueue(requestMsg)
	otherNodes := map[int]bool{}
	for nodeId, _ := range n.ptrMap {
		if nodeId == n.id {
			continue
		}
		otherNodes[nodeId] = false

	}
	n.replyTracker[n.logicalClock] = otherNodes
	n.broadcastMessage(requestMsg)

}

//Enter criticial section
func (n *Node) enterCS(msg Message) {
	fmt.Printf("[Node %d] <Entering CS> PQ: %s \n", n.id, printPQ(n.pq))
	//msg should be the request that is being granted the CS now

	//Simulate a random duration for the CS
	numSeconds := rand.Intn(5)
	fmt.Printf("[Node %d] Entering critical section for %d seconds for msg with priority %d \n", n.id, numSeconds, msg.timestamp)
	time.Sleep(time.Duration(numSeconds) * time.Second)
	fmt.Printf("[Node %d] Finished critical section in %d seconds \n", n.id, numSeconds)
	n.logicalClock += 1
	releaseMessage := Message{
		messageType: "Release",
		message:     "",
		senderID:    n.id,
		timestamp:   n.logicalClock,
		replyTarget: ReplyTarget{},
	}
	n.broadcastMessage(releaseMessage)
	delete(n.replyTracker, msg.timestamp)
	n.pq = n.pq[1:]
}

func (n *Node) replyMessage(receivedMsg Message) {
	fmt.Printf("Node %d is replying Node %d \n", n.id, receivedMsg.senderID)
	n.logicalClock += 1
	replyMessage := Message{
		messageType: "Reply",
		message:     "",
		senderID:    n.id,
		timestamp:   n.logicalClock,
		replyTarget: ReplyTarget{
			targetID:  receivedMsg.senderID,
			timestamp: receivedMsg.timestamp,
		},
	}

	n.sendMessage(replyMessage, receivedMsg.senderID)
}

func (n *Node) onReceiveRequest(msg Message) {
	//Check if i has received a reply from machine j for an earlier request
	// assert that the request's messageType = 0
	var replied bool = false
	if len(n.replyTracker) == 0 {
		go n.replyMessage(msg)
		replied = true
	}
	for requestTS, replyMap := range n.replyTracker {
		if requestTS < msg.timestamp {
			//Received the necessary reply
			if replyMap[msg.senderID] {
				//TODO: need goroutine?
				go n.replyMessage(msg)
				replied = true
			}

		} else if requestTS == msg.timestamp && n.id < msg.senderID {
			//Tiebreaker - there is a higher priority request AND ascertained that we received the necessary reply
			if replyMap[msg.senderID] {
				go n.replyMessage(msg)
				replied = true
			}
		} else {
			//	no earlier request
			go n.replyMessage(msg)
			replied = true
		}
	}
	if replied == false {
		//Add to a map of pending replies
		n.pendingReplies[msg.senderID] = append(n.pendingReplies[msg.senderID], msg)
	}
	//TODO: check if we need to enqueue no matter what - think so
	n.enqueue(msg)
	fmt.Printf("Node %d's PQ: %s \n", n.id, printPQ(n.pq))
}

func (n *Node) allReplied(timestamp int) bool {
	for _, replyStatus := range n.replyTracker[timestamp] {
		if replyStatus == false {
			return false
		}
	}
	return true
}

func (n *Node) getEmptyReplyMap() map[int]bool {
	ret := map[int]bool{}
	for i, _ := range n.ptrMap {
		if i == n.id {
			continue
		}
		ret[i] = false
	}
	return ret
}

func printPQ(pq []Message) string {
	var ret string = "|"
	for _, msg := range pq {
		ret += fmt.Sprintf("[TS: %d] by Node %d| ", msg.timestamp, msg.senderID)
	}
	return ret
}

func (n *Node) onReceiveReply(msg Message) {
	//Keep track in the replyTracker
	ts := msg.replyTarget.timestamp
	//if the ts does not exist in the replyTracker, create a entry for it
	if _, ok := n.replyTracker[ts]; !ok {
		n.replyTracker[ts] = n.getEmptyReplyMap()
	}
	n.replyTracker[msg.replyTarget.timestamp][msg.senderID] = true
	//Need to check if the node that replied has a pending request
	for _, msg := range n.pendingReplies[msg.senderID] {
		fmt.Printf("[Node %d] Node has a request waiting for reply...", msg.senderID)
		n.onReceiveRequest(msg)
	}
	// Check if everyone has replied this node
	if n.allReplied(msg.replyTarget.timestamp) {
		fmt.Printf("[Node %d] All replies have been received for Request with TS: %d \n", n.id, msg.replyTarget.timestamp)
		firstRequest := n.pq[0]
		if firstRequest.senderID == n.id && firstRequest.timestamp == msg.replyTarget.timestamp {
			//	TODO: Go Routine?
			fmt.Printf("[Node %d] Msg with timestamp %d is also at the front of the queue. \n[Node %d] will " +
				"now enter the CS. \n", n.id, msg.replyTarget.timestamp, n.id)
			n.enterCS(firstRequest)
		}

	}
}

func (n *Node) onReceiveRelease(msg Message) {
	if msg.senderID == n.pq[0].senderID {
		fmt.Printf("[Node %d] Before Length: %d \n", n.id, len(n.pq))
		n.pq = n.pq[1:]
		fmt.Printf("[Node %d] After Length: %d \n", n.id, len(n.pq))

	} else {
		fmt.Printf("[Node %d] Release msg from [Node %d] is not sent by the first request's "+
			"sender [Node %d] \n", n.id, msg.senderID, n.pq[0].senderID)

		return
	}

	if len(n.pq) > 0 {
		firstRequest := n.pq[0]
		if firstRequest.senderID == n.id {
			if n.allReplied(n.pq[0].timestamp) {
				n.enterCS(firstRequest)
			}
		}

	}
}

func (n *Node) broadcastMessage(msg Message) {
	for nodeId, _ := range n.ptrMap {
		if nodeId == n.id {
			continue
		}

		go n.sendMessage(msg, nodeId)
	}

}

func (n *Node) sendMessage(msg Message, receiverID int) {
	fmt.Printf("[Node %d] Sending a <%s> message to Node %d at MemAddr %p \n", n.id,
		msg.messageType, receiverID, n.ptrMap[receiverID])
	//Simulate uncertain latency and asynchronous nature of message passing
	numMilliSeconds := rand.Intn(2000)
	time.Sleep(time.Duration(numMilliSeconds) * time.Millisecond)
	receiver := n.ptrMap[receiverID]
	receiver.nodeChannel <- msg
}

func (n *Node) onReceivedMessage(msg Message) {

	//Taking the max(self.logicalClock, msg.timestamp) + 1
	if msg.timestamp >= n.logicalClock {
		n.logicalClock = msg.timestamp + 1
	} else {
		n.logicalClock += 1
	}

	fmt.Printf("[Node %d] Received a <%s> Message from Node %d \n", n.id, msg.messageType, msg.senderID)
	switch mType := msg.messageType; {
	case mType == "Request":
		n.onReceiveRequest(msg)

	case mType == "Reply":
		n.onReceiveReply(msg)

	case mType == "Release":
		n.onReceiveRelease(msg)
	}

	//Request MessageType = 0
	//Reply MessageType = 1
	//Release MessageType = 2
}

func (n *Node) listen() {
	for {
		select {
		case msg := <-n.nodeChannel:
			go n.onReceivedMessage(msg)
		}
	}
}

//Define constants here
const NUM_NODES int = 10

func main() {
	var automated bool

	for {
		fmt.Printf("There are two ways to run this programme.\n1. Automated [Press 1]\n" +
			"The Nodes will start to request to enter the Critical Section (CS) sequentially at a randomly spaced interval.\n" +
			"2. Interactive [Press 2] \n" +
			"You will control when the nodes request to enter the CS by pressing any key on the keyboard to start a Node's request\n")

		reader := bufio.NewReader(os.Stdin)
		char, _, _ := reader.ReadRune()
		if char == '1' {
			fmt.Println("You have chosen Automated, sit back and relax :)")
			automated = true
			time.Sleep(time.Duration(500) * time.Millisecond)
			break
		} else if char == '2' {
			fmt.Println("You have chosen Interactive")
			automated = false
			time.Sleep(time.Duration(500) * time.Millisecond)
			break

		} else {
			fmt.Println("Please enter either 1 or 2")
		}
	}

	fmt.Println("Automated: ", automated)
	//	TODO: Create a global address book
	globalNodeMap := map[int]*Node{}

	for i := 1; i <= NUM_NODES; i++ {
		newNode := NewNode(i)
		globalNodeMap[i] = newNode

	}
	//Give everyone the global pointer map
	for i := 1; i <= NUM_NODES; i++ {
		globalNodeMap[i].setPtrMap(globalNodeMap)
	}

	for i := 1; i <= NUM_NODES; i++ {
		go globalNodeMap[i].listen()
	}

	if automated {
		for i := 1; i <= NUM_NODES; i++ {
			//Insert a random probability
			numSeconds := rand.Intn(10)
			time.Sleep(time.Duration(numSeconds) * time.Second)
			go globalNodeMap[i].requestCS()
		}

	} else {
		for i := 1; i <= NUM_NODES; i++ {
			fmt.Println("Press [Enter] to get a new node to start requesting for CS")
			input := bufio.NewScanner(os.Stdin)
			input.Scan()
			go globalNodeMap[i].requestCS()
		}

	}


	time.Sleep(time.Duration(60) * time.Second)

}
