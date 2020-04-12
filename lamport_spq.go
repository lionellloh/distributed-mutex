package main

import "fmt"

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
	//A map to track the replies it has received for its own timestamp
	//{requestTimeStamp: {1: True} etc}
	replyTracker map[int] map[int] bool
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
	n := Node{id, 0, channel, pq, nil, replyTracker}

	return &n
}

func NewMessage(messageType MessageType, message string, senderID int, priority int) *Message {
	rt := ReplyTarget{nil, nil}
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
	for requestTS, replyMap := range n.replyTracker {
		if requestTS < msg.timestamp {
			//Received the necessary reply
			if replyMap[msg.senderID] {
				n.replyMessage(msg)
			}

		} else if requestTS == msg.timestamp && n.id < msg.senderID {
			//Received the necessary reply
			if replyMap[msg.senderID] {
				n.replyMessage(msg)
			}
		} else {
		//	no earlier request
			n.replyMessage(msg)
		}
	}

	n.enqueue(msg)
}

func (n *Node) onReceiveReply(msg Message){
	//Need to note for which Id is the message for

	n.pq = append(n.pq, msg)

}

func (n *Node) onReceiveRelease(msg Message) {

}

func (n *Node) exitCS(){
	// sendMessage to everyone here
}


func (n *Node) broadcastMessage(msg Message){
	for nodeId, nodeAddr := range n.ptrMap {
		if nodeId == n.id {
			continue
		}
		fmt.Printf("Sending to %s at %s", nodeId, nodeAddr)
		(*nodeAddr).nodeChannel <- msg
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
				n.onMessageReceived(msg)
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

	// How to decide who wants to enter the critical section?

}
