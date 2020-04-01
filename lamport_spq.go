package main

import "fmt"

//Code for implementing Distributed Mutual Exclusion through Lamport's Shared Priority Queue

type Node struct {
	id int
	logicalClock int
	nodeChannel chan Message


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



func newNode(id int) *Node {
	channel := make(chan Message)
	n := Node{id, 0, channel }

	return &n
}

func newMessage(messageType MessageType, message string, receiverID int) *Message {
	m := Message{messageType, message, receiverID}

	return &m
}

//Given an array of nodes, return an array of channels



func main() {
	fmt.Println(MessageType(Request))
}