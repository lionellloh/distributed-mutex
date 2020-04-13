package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type MessageType string

const (
	Request MessageType = "Request"
	Reply   MessageType = "Release"
	Release MessageType = "Grant"
)

type Server struct {
	serverChannel chan Message
	queue         []Message
	ptrMap        map[int]*Node
	quit 	      chan int
}

type Node struct {
	id 			int
	nodeChannel chan Message
	server      Server
	wg 			*sync.WaitGroup
	quit 		chan int
}

type Message struct {
	messageType MessageType
	senderID    int
}

func (s *Server) enqueue(msg Message) {
	s.queue = append(s.queue, msg)
	fmt.Printf("[Server] Enqueue called - State of Queue: %s \n", stringPQ(s.queue))
}

func (s *Server) dequeue() {
	s.queue = s.queue[1:]
	fmt.Printf("[Server] Dequeued called - State of Queue: %s \n", stringPQ(s.queue))
}

func (s *Server) sendMessage(msg Message, recipientID int) {
	fmt.Printf("[Server] is sending a %s message to [Node %d] \n", msg.messageType, recipientID)
	numMilliSeconds := rand.Intn(2000)
	time.Sleep(time.Duration(numMilliSeconds) * time.Millisecond)
	recipientNode := s.ptrMap[recipientID]
	recipientNode.nodeChannel <- msg
}

func (n *Node) requestCS() {
	fmt.Printf("=======================================\n Node %d is " +
		"requesting to enter the CS \n=======================================\n", n.id)
	fmt.Println()
	requestMsg := Message{
		messageType: "Request",
		senderID:    n.id,
	}
	n.sendMessage(requestMsg)
}

func stringPQ(pq []Message) string {
	if len(pq) == 0 {
		return "PQ: | Empty |"
	}
	var ret = "| "
	for _, msg := range pq {
		ret += fmt.Sprintf("Node %d| ", msg.senderID)
	}
	return ret
}

func (n *Node) sendMessage(msg Message) {
	fmt.Printf("[Node %d] is sending a %s message to [Server] \n", n.id, msg.messageType)
	numMilliSeconds := rand.Intn(2000)
	time.Sleep(time.Duration(numMilliSeconds) * time.Millisecond)
	n.server.serverChannel <- msg
}


func (n *Node) enterCS(){
	numSeconds := rand.Intn(3)
	fmt.Printf("[Node %d] is entering CS \n", n.id)
	time.Sleep(time.Duration(numSeconds) * time.Second)

	//leave CS
	fmt.Printf("[Node %d] exited the CS after %d seconds. \n", n.id, numSeconds)

	releaseMsg := Message{
		messageType: "Release",
		senderID:    n.id,
	}
	n.sendMessage(releaseMsg)
	n.wg.Done()
}



func (n *Node) listen() {
	for {
		select {
		case msg := <- n.nodeChannel:
			fmt.Printf("[Node %d] received a %s message from server \n", n.id, msg.messageType)
			if msg.messageType == "Grant" {
				n.enterCS()
			}

		case <- n.quit:
			return
		}
	}
}


func (s *Server) listen() {

	grantMessage := Message{
		messageType: "Grant",
		senderID:    0,
	}

	for {
		select {
			case <- s.quit:
				return
			case msg := <-s.serverChannel:
			fmt.Printf("[Server] received a %s message from [Node %d] \n", msg.messageType, msg.senderID)
			if msg.messageType == "Request" {
				go s.enqueue(msg)
				if len(s.queue) == 0 {
					go s.sendMessage(grantMessage, msg.senderID)
				}
			} else if msg.messageType == "Release" {
				s.dequeue()
				if len(s.queue) > 0 {
					nextNodeID := s.queue[0].senderID
					go s.sendMessage(grantMessage, nextNodeID)
				}
			}
		}
	}
}

const NUM_NODES int = 10

func main(){
	var wg sync.WaitGroup

	globalNodeMap := map[int] *Node {}
	server := Server{
		serverChannel: make(chan Message),
		queue:         [] Message{},
		ptrMap: 	   map[int]*Node{},
	}
	for i := 1; i <= NUM_NODES; i++ {
		newNode := Node{
			id:          i,
			nodeChannel: make(chan Message),
			server:      server,
		}
		globalNodeMap[i] = &newNode
	}

	server.ptrMap = globalNodeMap
	go server.listen()
	for i := 1; i <= NUM_NODES; i++ {
		go globalNodeMap[i].listen()
		wg.Add(1)
		globalNodeMap[i].wg = &wg
	}

	start := time.Now()
	for i := 1; i <= NUM_NODES; i++ {
		go globalNodeMap[i].requestCS()
	}

	wg.Wait()
	t:= time.Now()
	time.Sleep(time.Duration(3) * time.Second)
	fmt.Printf("Time Taken: %.2f seconds \n", t.Sub(start).Seconds())

	for i := 1; i <= NUM_NODES; i++ {
		globalNodeMap[i].quit <- 1

	}
	server.quit <- 1
	fmt.Println("All Nodes have entered entered and exited the Critical Section\nEnding programme now.\n")

}


