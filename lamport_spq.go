package main

import (
	"fmt"
	"math/rand"
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
	id int
	logicalClock int
	nodeChannel chan Message
	pq []Message
	ptrMap map[int] *Node
	//A map to track the replies the node has received for its own request stamped with timestamp t
	//{requestTimeStamp: {nodeId: True} etc}
	replyTracker map[int] map[int] bool
	//requestTimeStamp: {nodeId: []Message}
	pendingReplies map[int] []Message
}

type MessageType int
const (
	Request MessageType = 0
	Reply MessageType = 1
	Release MessageType = 2
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
	targetID int
	timestamp int
}

//Constructor functions

func NewNode(id int) *Node {
	channel := make(chan Message)
	var pq []Message
	//Create a blank map to track
	var replyTracker = map[int] map[int] bool{}
	n := Node{id, 0, channel, pq, nil, replyTracker, map[int] []Message{}}

	return &n
}

func NewMessage(messageType MessageType, message string, senderID int, priority int) *Message {
	rt := ReplyTarget{-1, -1}
	m := Message{messageType, message, senderID, priority, rt}

	return &m
}


//Struct Methods
func (n *Node) setPtrMap(ptrMap map[int]*Node){
	n.ptrMap = ptrMap
}

func (n *Node) enqueue(m Message){
	//TODO: Write the logic for inserting a new message in the correct position
	n.pq = append(n.pq, m)
}

//TODO: request to enter CS
//TODO: How to simulate a node's need to enter the CS?
func (n *Node) requestCS(){
	fmt.Printf("Node %d is requesting to enter the CS \n", n.id)
	requestMsg := Message{
		messageType: 1,
		message:     "",
		senderID:    n.id,
		timestamp:   n.logicalClock,
	}
	n.enqueue(requestMsg)
	otherNodes := map[int] bool{}
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
	//msg should be the request that is being granted the CS now
	numSeconds := rand.Intn(10)
	fmt.Printf("Node %s is entering critical section for %d seconds for msg with priority %d", n.id, numSeconds, msg.timestamp)
	time.Sleep(time.Duration(numSeconds) * time.Second)
	fmt.Printf("Node %s is done with critical section for %d seconds", numSeconds)

	releaseMessage := Message{
		messageType: 2,
		message:     "",
		senderID:    n.id,
		timestamp:   n.logicalClock,
		replyTarget: ReplyTarget{},
	}
	n.broadcastMessage(releaseMessage)
	delete(n.replyTracker, msg.timestamp)
	n.pq = n.pq[1:]
}

func (n *Node) replyMessage(receivedMsg Message){
	replyMessage := Message{
		messageType: 1,
		message:     "",
		senderID:    n.id,
		timestamp:   n.logicalClock,
		replyTarget: ReplyTarget{
			targetID:  receivedMsg.senderID,
			timestamp: receivedMsg.timestamp,
		},
	}

	n.sendMessage(replyMessage, replyMessage.senderID)
}

func (n *Node) onReceiveRequest(msg Message){
	//Check if i has received a reply from machine j for an earlier request
	// assert that the request's messageType = 0
	var replied bool = false
	for requestTS, replyMap := range n.replyTracker {
		if requestTS < msg.timestamp {
			//Received the necessary reply
			if replyMap[msg.senderID] {
				//TODO: need goroutine?
				n.replyMessage(msg)
				replied = true
			}

		} else if requestTS == msg.timestamp && n.id < msg.senderID {
			//Tiebreaker - there is a higher priority request AND ascertained that we received the necessary reply
			if replyMap[msg.senderID] {
				n.replyMessage(msg)
				replied = true
			}
		} else {
		//	no earlier request
			n.replyMessage(msg)
			replied = true
		}
	}

	if !replied {
		//Add to a map of pending replies
		n.pendingReplies[msg.senderID] = append(n.pendingReplies[msg.senderID], msg)
	}


	//TODO: check if we need to enqueue no matter what - think so
	n.enqueue(msg)
}

func (n *Node) allReplied(timestamp int) bool {
	for _, replyStatus := range n.replyTracker[timestamp] {
		if replyStatus == false {
			return false
		}
	}
	return true
}

func (n *Node) onReceiveReply(msg Message){
	//Keep track in the replyTracker
	n.replyTracker[msg.replyTarget.timestamp][msg.senderID] = true
	//Need to check if the node that replied has a pending request
	for _, msg := range n.pendingReplies[msg.senderID] {
		fmt.Printf("Node %s has a message waiting for reply...", msg.senderID)
		n.onReceiveRequest(msg)
	}
	// Check if everyone has replied this node
	if n.allReplied(msg.replyTarget.timestamp)  {
		firstRequest := n.pq[0]
		if firstRequest.senderID == n.id && firstRequest.timestamp == msg.replyTarget.timestamp {
			//	TODO: Go Routine?
			n.enterCS(firstRequest)
		}


	}
}

func (n *Node) onReceiveRelease(msg Message) {
	if msg.senderID == n.pq[0].senderID {

	} else {
		fmt.Printf("Release msg [Node %s] is not sent by the first request's " +
			"sender [Node %s] \n", msg.senderID, n.pq[0].senderID)

		return
	}
	n.pq = n.pq[1:]
	firstRequest := n.pq[0]
	if firstRequest.senderID == n.id{
		if n.allReplied(n.pq[0].timestamp){
			n.enterCS(firstRequest)
		}
	}
}


func (n *Node) broadcastMessage(msg Message){
	for nodeId, nodeAddr := range n.ptrMap {
		fmt.Println(n.id, nodeId)
		if nodeId == n.id {
			continue
		}
		fmt.Printf("Node %d sending to Node %d at %p \n", n.id, nodeId, nodeAddr)
		go n.sendMessage(msg, nodeId)
	}

}

func (n *Node) sendMessage(msg Message, receiverID int){
	receiver := n.ptrMap[receiverID]
	receiver.nodeChannel <- msg
}


func (n *Node) onMessageReceived(msg Message){
	//Message is a request by another node
	switch mType := msg.messageType; {
	case mType == 0:
		fmt.Println("0 Request")
		n.onReceiveRequest(msg)

		case mType == 1:
			fmt.Println("1 Reply")
			n.onReceiveReply(msg)

		case mType == 2:
			fmt.Println("2 Release")
		}

		//Request MessageType = 0
		//Reply MessageType = 1
		//Release MessageType = 2
	}

func (n *Node) listen(){
	for {
		select {
			case msg := <- n.nodeChannel:
				go n.onMessageReceived(msg)
		}
	}
}

//Define constants here
const NUM_NODES int = 10;

func main() {
//	TODO: Create a global address book
	globalNodeMap := map[int] *Node {}

	for i:=1; i<=NUM_NODES; i++{
		newNode := NewNode(i)
		globalNodeMap[i] = newNode

	}
	//Give everyone the global pointer map
	for i:=1; i<=NUM_NODES; i++ {
		globalNodeMap[i].setPtrMap(globalNodeMap)
	}

	for i:=1; i<=NUM_NODES; i++ {
		go globalNodeMap[i].listen()
	}

	for i:=1; i<=NUM_NODES; i++ {
		numSeconds := rand.Intn(10)
		time.Sleep(time.Duration(numSeconds) * time.Second)
		//Insert a random probability
		go globalNodeMap[i].requestCS()
	}

	time.Sleep(time.Duration(30) * time.Second)
	// How to decide who wants to enter the critical section?

}
