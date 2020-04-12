package main

import "fmt"

//Code for implementing Distributed Mutual Exclusion through Lamport's Shared Priority Queue

//INSTRUCTIONS:

/*

Using the GO language, implement the following distributed mutual exclusion protocols using at least ten (10) nodes:
1. (10 marks) Lamport’s shared priority queue without Ricart and Agrawala’s optimization.
2. (10 marks) Lamport’s shared priority queue with Ricart and Agrawala’s optimization.
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
}

type MessageType int
const (
	Request MessageType = 0
	Reply MessageType = 1
	Release MessageType = 2
)

type Message struct {
	messageType MessageType
	message string
	receiverID int
	priority int

}

//Constructor functions

func NewNode(id int) *Node {
	channel := make(chan Message)
	var pq []Message
	n := Node{id, 0, channel, pq, nil}

	return &n
}

func NewMessage(messageType MessageType, message string, receiverID int, priority int) *Message {
	m := Message{messageType, message, receiverID, priority}

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
	// do some switch thing here

}

func (n *Node) onReceiveRequest(msg Message){

	// assert that the request's messageType = 0
	n.pq = append(n.pq, msg)


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

func (n *Node) sendMessage(msg Message, receiver Node){
	receiver.nodeChannel <- msg
}


func (n *Node) onMessageReceived(msg Message){
	//Message is a request by another node
	switch mType := msg.messageType; {
	case mType == 0:
		fmt.Println("0")

	case mType == 1:
		fmt.Println("1")

	case mType == 2:
		fmt.Println("2")
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
