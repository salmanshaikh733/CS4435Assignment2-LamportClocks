package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	pb "lamportClocks/proto"
)

type node struct {
	// Self information
	Name string
	Addr string

	// Consul related variables
	SDAddress string
	SDKV      api.KV

	// used to make requests
	Clients map[string]pb.MessageServiceClient

	Filename string
}

type BankAccount struct {
	Name      string
	AccountID int64
	Balance   float32
}

type Instruction struct {
	Action               string
	AccountID            int64
	Amount               float32
	transactionTimeStamp int64
}

//declare global variables
var accounts []BankAccount
var instructions []Instruction
var t = 0
var numNodes = 0
var numAcks = 0
var doneAcks = 0
var doneFlag = false

// SendMessage implements helloworld.GreeterServer -- when other node contacts this node
func (n *node) SendMessage(ctx context.Context, in *pb.MessageRequest) (*pb.MessageResponse, error) {

	if in.Action == "done" {
		doneAcks++
		t++
		var maxT = Max(int64(t), in.T)
		if doneFlag == false {
			return &pb.MessageResponse{T: maxT, OtherNodeDoneFlag: false}, nil
		} else {
			return &pb.MessageResponse{T: maxT, OtherNodeDoneFlag: true}, nil
		}

		return &pb.MessageResponse{T: maxT, Ack: "Acknowledged you are finished i see"}, nil
	} else {
		t++
		var maxT = Max(int64(t), in.T)
		//add event to queue
		bankInstruction := Instruction{
			Action:               in.Action,
			AccountID:            in.AccountNum,
			Amount:               float32(in.Amount),
			transactionTimeStamp: maxT,
		}
		println("received message ----------")
		println(bankInstruction.transactionTimeStamp)
		println(bankInstruction.Action)
		println("end of receive---------------")

		instructions = append(instructions, bankInstruction)

		//return acknowledged that transaction has been added to queue
		return &pb.MessageResponse{T: int64(t), Ack: "Event Acknowledged from " + n.Name}, nil
	}
}

func Max(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

// Start the node.
// This starts listening at the configured address. It also sets up clients for it's peers.
func (n *node) Start() {
	// init required variables
	n.Clients = make(map[string]pb.MessageServiceClient)

	// start service / listening
	go n.StartListening()

	// register with the service discovery unit
	n.registerService()

	// start the main loop here
	// in our case, simply time out for 1 minute and greet all

	// wait for other nodes to come up - infinite loop

	for {
		time.Sleep(20 * time.Second)
		n.getNumberOfNodes()
		time.Sleep(20 * time.Second)
		n.GreetAll()
	}
}

func (n *node) getNumberOfNodes() {
	kvpairs, _, err := n.SDKV.List("Node", nil)

	if err != nil {
		log.Panicln(err)
		return
	}

	for _, kventry := range kvpairs {
		if strings.Compare(kventry.Key, n.Name) == 0 {
			// ourself
			continue
		}
		if n.Clients[kventry.Key] == nil {
			fmt.Println("New member: ", kventry.Key)
			// connection not established previously
			numNodes++
		}
	}
}

// Start listening/service.
func (n *node) StartListening() {

	lis, err := net.Listen("tcp", n.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	_n := grpc.NewServer() // n is for serving purpose

	pb.RegisterMessageServiceServer(_n, n)
	// Register reflection service on gRPC server.
	reflection.Register(_n)

	// start listening
	if err := _n.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// Register self with the service discovery module.
// This implementation simply uses the key-value store. One major drawback is that when nodes crash. nothing is updated on the key-value store. Services are a better fit and should be used eventually.
func (n *node) registerService() {
	config := api.DefaultConfig()
	config.Address = n.SDAddress
	consul, err := api.NewClient(config)
	if err != nil {
		log.Panicln("Unable to contact Service Discovery.")
	}

	kv := consul.KV()
	p := &api.KVPair{Key: n.Name, Value: []byte(n.Addr)}
	_, err = kv.Put(p, nil)
	if err != nil {
		log.Panicln("Unable to register with Service Discovery.")
	}

	// store the kv for future use
	n.SDKV = *kv

	log.Println("Successfully registered with Consul.")
}

// Setup a new grpc client for contacting the server at addr.
/*func (n *node) SetupClient(name string, addr string) {

	// setup connection with other node
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	n.Clients[name] = pb.NewMessageServiceClient(conn)

	// send message from other node
	r, err := n.Clients[name].SendMessage(context.Background(), &pb.MessageRequest{Message: n.Name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting from the other node: %s", r.Ack)

}*/

// Busy Work module, greet every new member you find
func (n *node) GreetAll() {

	// get all nodes -- inefficient, but this is just an example
	kvpairs, _, err := n.SDKV.List("Node", nil)
	if err != nil {
		log.Panicln(err)
		return
	}

	instructionFile, err := os.Open("./bin/" + n.Filename)

	if err != nil {
		log.Fatalf("could not open file")
	}

	defer instructionFile.Close()

	scanner := bufio.NewScanner(instructionFile)

	for scanner.Scan() {
		input := strings.Fields(scanner.Text())
		println(input[0] + "\n")
		//if last line
		if input[0] == "done" {
			t++
			//done flag tells us that this node is done processing transactions.
			doneFlag = true
			//TODO mistake here make sure to add timestamp when adding done
			//lastIns := Instruction{Action: input[0], transactionTimeStamp: int64(t)}
			//instructions = append(instructions, lastIns)

			//for every node do send a done ack
			for _, kventry := range kvpairs {
				println("Sending done ack")
				//if ourselves
				if strings.Compare(kventry.Key, n.Name) == 0 {
					continue
				}
				// connection not established previously
				if n.Clients[kventry.Key] == nil {
					//n.SetupClient(kventry.Key, string(kventry.Value))
					conn, err := grpc.Dial(string(kventry.Value), grpc.WithInsecure())
					if err != nil {
						log.Fatalf("did not connect: %v", err)
					}
					defer conn.Close()
					n.Clients[kventry.Key] = pb.NewMessageServiceClient(conn)

					// send message from other node
					r, err := n.Clients[kventry.Key].SendMessage(context.Background(), &pb.MessageRequest{Action: "done"})
					if err != nil {
						log.Fatalf("could not greet: %v", err)
					}

					//for each successful return increment numAcks for the current node

					log.Printf("Ack received from other node, transaction verified: %s", r.Ack)

				}
			}
			//break out of loop
			break
		} else {
			println("Sending normal ack")
			action := input[0]
			accountNum, _ := strconv.ParseInt(input[1], 10, 64)
			amount, _ := strconv.ParseInt(input[2], 10, 64)
			t++

			bankInstruction := Instruction{
				Action:               action,
				AccountID:            accountNum,
				Amount:               float32(amount),
				transactionTimeStamp: int64(t),
			}

			//for each transaction increment t

			//add transaction to queue
			instructions = append(instructions, bankInstruction)

			//reset acks for each transaction
			numAcks = 0
			// fmt.Println("Found nodes: ") -- for each node
			for _, kventry := range kvpairs {
				if strings.Compare(kventry.Key, n.Name) == 0 {
					// ourself
					continue
				}
				//TODO issue over here
				if n.Clients[kventry.Key] == nil {
					fmt.Println("New member: ", kventry.Key)
					// connection not established previously
					//n.SetupClient(kventry.Key, string(kventry.Value))

					///////////////////////////
					conn, err := grpc.Dial(string(kventry.Value), grpc.WithInsecure())
					if err != nil {
						log.Fatalf("did not connect to node : %v", err)
					}
					defer conn.Close()
					n.Clients[kventry.Key] = pb.NewMessageServiceClient(conn)

					// send message from other node
					r, err := n.Clients[kventry.Key].SendMessage(context.Background(), &pb.MessageRequest{Action: action, AccountNum: accountNum, Amount: amount, T: int64(t)})
					if err != nil {
						log.Fatalf("could not send transaction to node: %v", err)
					}

					//for each successful return increment numAcks for the current node
					numAcks++

					//close connection
					n.Clients[kventry.Key] = nil
					log.Printf("Ack received from other node, transaction verified: %s", r.Ack)
				}
			}
		}
	}
	//println(numAcks, numNodes, doneAcks)
	//infinite loop waiting for other nodes to finish
	//TODO issue over here - since we are not sending messages to other nodes done acks is not getting incremented. therefore stuck in infinite loop
	for {
		if doneAcks == numNodes {
			//sort the queue
			sort.SliceStable(instructions, func(i, j int) bool {
				return instructions[i].transactionTimeStamp < instructions[j].transactionTimeStamp
			})
			fmt.Printf("%v", instructions)
			if numAcks == numNodes {
				for i := range instructions {
					//if deposit
					if instructions[i].Action == "deposit" {
						for j := range accounts {
							if accounts[j].AccountID == instructions[i].AccountID {
								accounts[j].Balance = accounts[j].Balance + instructions[i].Amount
							}
						}
					} else if instructions[i].Action == "withdraw" {
						for j := range accounts {
							if accounts[j].AccountID == instructions[i].AccountID {
								accounts[j].Balance = accounts[j].Balance - instructions[i].Amount
							}
						}
					} else if instructions[i].Action == "interest" {
						for j := range accounts {
							if accounts[j].AccountID == instructions[i].AccountID {
								accounts[j].Balance = accounts[j].Balance + ((instructions[i].Amount / 100) * accounts[j].Balance)
							}
						}
					} else if instructions[i].Action == "done" {
						break
					}
				}
				println("writing new json")
				//write new updated accounts json file
				dat, _ := json.MarshalIndent(accounts, "", "")
				var updateFileName = "./bin/accountsUpdated" + n.Name + ".json"
				ioutil.WriteFile(updateFileName, dat, 0644)
				//done operating transactions
				os.Exit(0)
			}
		} else {
			continue
		}
	}
	//done reading all files

	//

}

func main() {
	file, err := ioutil.ReadFile("./bin/accounts.json")

	if err != nil {
		fmt.Println(err.Error())
	}

	//store json data in too global variable accounts
	err2 := json.Unmarshal(file, &accounts)

	if err2 != nil {
		fmt.Println(err2)
	}

	// pass the port as an argument and also the port of the other node
	args := os.Args[1:]

	if len(args) < 4 {
		fmt.Println("Arguments required: <name> <listening address> <consul address> and input file name")
		os.Exit(1)
	}

	// args in order
	name := args[0]
	listenaddr := args[1]
	sdaddress := args[2]
	fileName := args[3]

	/*instructionFile, err := os.Open("./bin/" + fileName)

	if err != nil {
		log.Fatalf("could not open file")
	}

	defer instructionFile.Close()

	scanner := bufio.NewScanner(instructionFile)

	//load instructions into instructions array for specific node

	for scanner.Scan() {
		input := strings.Fields(scanner.Text())

		//if last line
		if input[0] == "done" {
			lastIns := Instruction{Action: input[0]}
			instructions = append(instructions, lastIns)
			break
		}
		action := input[0]
		accountNum, _ := strconv.ParseInt(input[1], 10, 64)
		amount, _ := strconv.ParseInt(input[2], 10, 64)

		bankInstruction := Instruction{
			Action:    action,
			AccountID: accountNum,
			Amount:    float32(amount),
		}

		instructions = append(instructions, bankInstruction)
	}*/

	noden := node{Name: name, Addr: listenaddr, SDAddress: sdaddress, Clients: nil, Filename: fileName} // node n is for operational purposes

	// start the node
	noden.Start()
}
