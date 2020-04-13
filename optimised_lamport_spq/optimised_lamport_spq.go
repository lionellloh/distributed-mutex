package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
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
	globalWG *sync.WaitGroup
	quit chan int
}

type MessageType string

const (
	Request MessageType = "Request"
	Reply   MessageType = "Reply"
	Release MessageType = "ReleaseReply"
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
	n := Node{id, 0, channel, pq, nil,
		replyTracker, &sync.WaitGroup{}, make(chan int)}

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
	fmt.Printf("[Node %d] BEFORE: %s \n", n.id, stringPQ(n.pq))

	for _, msg := range n.pq {
		if newMsg.senderID == msg.senderID {
			return
		}
	}
	n.pq = append(n.pq, newMsg)
	sort.SliceStable(n.pq, func(i,j int) bool {
		if n.pq[i].timestamp < n.pq[j].timestamp {
			return true
		} else if n.pq[i].timestamp == n.pq[j].timestamp && n.pq[i].senderID < n.pq[i].senderID {
			return true
		} else{
			return false
		}
	})

	fmt.Printf("[Node %d] AFTER: %s \n", n.id, stringPQ(n.pq))
}
//One MSG from a node in the pq at any one time
func (n *Node) dequeue(senderID int) {
	for i, msg := range n.pq {
		if msg.senderID == senderID {
			if i >= len(n.pq) -1 { //Handle edge case where it's the only element or last element
				n.pq = n.pq[:i]

			} else {
				n.pq = append(n.pq[:i], n.pq[i+1:]...)
			}
			return
		}
	}
}

func (n *Node) requestCS() {
	n.logicalClock += 1

	if NUM_NODES == 1 {
		n.enterCS(Message{
			messageType: "Request",
			message:     "",
			senderID:    n.id,
			timestamp:   n.logicalClock,
			replyTarget: ReplyTarget{},
		})
	}
	fmt.Printf("=======================================\n Node %d is " +
		"requesting to enter the CS \n =======================================\n", n.id)
	time.Sleep(time.Duration(500) * time.Millisecond)

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
	defer n.globalWG.Done()
	//Should have at least 1 request
	fmt.Printf("[Node %d] <Entering CS> %s \n", n.id, stringPQ(n.pq))
	n.dequeue(msg.senderID) //dequeue at the start to avoid race conditions
	//msg should be the request that is being granted the CS now

	//Simulate a random duration for the CS
	numSeconds := 1
	fmt.Printf("[Node %d] Entering critical section for %d seconds for msg with priority %d \n", n.id, numSeconds, msg.timestamp)
	time.Sleep(time.Duration(numSeconds) * time.Second)
	fmt.Printf("[Node %d] Finished critical section in %d seconds \n", n.id, numSeconds)
	n.logicalClock += 1
	fmt.Printf("[Node %d] After finishing CS, PQ: %s \n", n.id, stringPQ(n.pq))
	for _, reqMsg := range n.pq {
		releaseMessage := Message{
			messageType: "ReleaseReply",
			message:     "",
			senderID:    n.id,
			timestamp:   n.logicalClock,
			replyTarget: ReplyTarget{reqMsg.senderID, reqMsg.timestamp},
		}

		go n.sendMessage(releaseMessage, reqMsg.senderID)
	}
	//Empty the PQ
	n.pq = []Message{}

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
	time.Sleep(time.Duration(50) * time.Millisecond)
	var earlierRequestExists bool = false

	for _, requestMsg := range n.pq {
		//If it is the node's own message
		if requestMsg.senderID == n.id {
			if requestMsg.timestamp < msg.timestamp {
				earlierRequestExists = true
				break
			}  else if requestMsg.timestamp == msg.timestamp && requestMsg.senderID < msg.senderID {
				earlierRequestExists = true
				break
			}
		}
	}

	if earlierRequestExists {
		n.enqueue(msg)
	} else {
		go n.replyMessage(msg)
	}

	fmt.Printf("Node %d's PQ: %s \n", n.id, stringPQ(n.pq))
}

func (n *Node) allReplied(timestamp int) bool {
	//Ensure the edge case that if the timestamp does not even exist, we do not considered that it has received all its reply
	if _, ok := n.replyTracker[timestamp]; !ok {
		return false
	}
	for _, replyStatus := range n.replyTracker[timestamp] {
		if replyStatus == false {
			return false
		}
	}
	//Moved this here so we do not have a case where multiple replies trigger a check a

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

func stringPQ(pq []Message) string {
	if len(pq) == 0 {
		return "PQ: | Empty |"
	}
	var ret = "|"
	for _, msg := range pq {
		ret += fmt.Sprintf("PQ: [TS: %d] by Node %d| ", msg.timestamp, msg.senderID)
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
	// Check if everyone has replied this node
	if n.allReplied(msg.replyTarget.timestamp) {
		//reset
		delete(n.replyTracker, msg.replyTarget.timestamp)
		fmt.Printf("[Node %d] All replies have been received for Request with TS: %d \n", n.id, msg.replyTarget.timestamp)
		firstRequest := n.pq[0]
		n.enterCS(firstRequest)
	}
}
//New Handler function to due with the combined message
func (n *Node) onReceiveReleaseReply(msg Message) {
	//Mark the reply map
	ts := msg.replyTarget.timestamp
	//if the ts does not exist in the replyTracker, create a entry for it
	if _, ok := n.replyTracker[ts]; !ok {
		n.replyTracker[ts] = n.getEmptyReplyMap()
	}
	n.replyTracker[msg.replyTarget.timestamp][msg.senderID] = true
	fmt.Printf("[Node %d] PQ: %s \n", n.id, stringPQ(n.pq))
	if n.allReplied(n.pq[0].timestamp) {
		//Reset
		delete(n.replyTracker, msg.replyTarget.timestamp)
		n.enterCS(n.pq[0])
	}
}

func (n *Node) broadcastMessage(msg Message) {

	{
		for nodeId, _ := range n.ptrMap {
			if nodeId == n.id {
				continue
			}
			go n.sendMessage(msg, nodeId)
		}
	}
}

func (n *Node) sendMessage(msg Message, receiverID int) {
	fmt.Printf("[Node %d] Sending a <%s> message to Node %d at MemAddr %p \n", n.id,
		msg.messageType, receiverID, n.ptrMap[receiverID])
	//Simulate uncertain latency and asynchronous nature of message passing
	numMilliSeconds := rand.Intn(1000) + 2000
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

	case mType == "ReleaseReply":
		n.onReceiveReleaseReply(msg)
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
		case <-n.quit:
			return
		}
	}
}

//Define constants here
const NUM_NODES int = 1

func main() {
	var wg sync.WaitGroup
	var automated bool

	for {
		fmt.Printf("There are two ways to run this programme.\n1. Automated [Press 1]\n" +
			"The Nodes will start to request to enter the Critical Section (CS) concurrently.\n" +
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
	globalNodeMap := map[int]*Node{}

	for i := 1; i <= NUM_NODES; i++ {
		newNode := NewNode(i)
		globalNodeMap[i] = newNode

	}
	//Give everyone the global pointer map
	for i := 1; i <= NUM_NODES; i++ {
		wg.Add(1)
		globalNodeMap[i].setPtrMap(globalNodeMap)
	}

	for i := 1; i <= NUM_NODES; i++ {
		globalNodeMap[i].globalWG = &wg
		go globalNodeMap[i].listen()
	}

	start := time.Now()
	if automated {

		for i := 1; i <= NUM_NODES; i++ {
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

	wg.Wait()
	t:= time.Now()
	time.Sleep(time.Duration(3) * time.Second)

	fmt.Printf("Number of nodes: %d \n", NUM_NODES)
	fmt.Printf("Time Taken: %.2f seconds \n", t.Sub(start).Seconds())


	fmt.Println("All Nodes have entered entered and exited the Critical Section\nEnding programme now.\n")
	for i := 1; i <= NUM_NODES; i++ {
		globalNodeMap[i].quit <- 1
	}

}