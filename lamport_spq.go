package main

import (
	//"container/heap"
	"fmt"
)

//Code for implementing Distributed Mutual Exclusion through Lamport's Shared Priority Queue

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
	pq := []int{}
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

func (n *Node) exitCS(){
	// sendMessage to everyone here
}

func (n *Node) sendMessage(){
	//base level method to send message
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

}
