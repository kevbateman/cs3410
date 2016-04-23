package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

var chatty bool
var latency int
var dumper bool

// Replica ...
type Replica struct {
	Address   string
	Data      map[string]string
	Cell      []string
	Slots     []Slot
	ToApply   Slot
	Listeners map[string]chan string
}

// Slot ...
type Slot struct {
	Sequence Sequence
	Command  Command
	Decided  bool
	Index    int
}

// Command ...
type Command struct {
	Address         string
	Command         string
	Sequence        Sequence
	Key             string
	UniqueRandomTag int64
}

// Sequence ...
type Sequence struct {
	Number  int
	Address string
}

// Prepare ...
type Prepare struct {
	Slot Slot
	Seq  Sequence
}

// Accepted ...
type Accepted struct {
	Command Command
	Slot    Slot
	Seq     Sequence
}

// Propose ...
type Propose struct {
	Command Command
}

// Accept ...
type Accept struct {
	Slot Slot
	N    Sequence
	V    Command
}

// Decide ...
type Decide struct {
	Slot  Slot
	Value Command
}

// Request ...
type Request struct {
	Prepare  Prepare
	Accepted Accepted
	Propose  Propose
	Accept   Accept
	Decide   Decide
	Message  string
	Address  string
}

// Response ...
type Response struct {
	Message  string
	Okay     bool
	Command  Command
	Promised Sequence
}

func allDecided(slots []Slot, n int) bool {
	if n == 1 {
		return true
	}
	for k, v := range slots {
		if k == 0 {
			continue
		}
		if k >= n {
			break
		}
		if !v.Decided {
			return false
		}
	}
	return true
}

// Dumpall ...
func (elt *Replica) Dumpall(_ Request, _ *Response) error {
	log.Println("Data")
	for k, v := range elt.Data {
		fmt.Printf("\tKey: %s, Value: %s\n", k, v)
	}
	log.Println("Cell")
	for k, v := range elt.Slots {
		if v.Decided {
			fmt.Printf("\t{%d,%v}\n", k, v)
		}
	}
	return nil
}

func call(address, method string, request Request, response *Response) error {
	client, err := rpc.DialHTTP("tcp", getAddress(address))
	if err != nil {
		log.Println("rpc dial: ", err)
		return err
	}
	defer client.Close()

	err = client.Call("Replica."+method, request, response)
	if err != nil {
		log.Println("rpc call: ", err)
		return err
	}
	return nil
}

func getAddress(v string) string {
	return net.JoinHostPort("localhost", v)
}

func majority(size int) int {
	return int(math.Ceil(float64(size) / 2))
}

func chatting(message string) {
	if chatty {
		log.Println(message)
	}
}

// String converts to string and prints contents of struct
func (elt Sequence) String() string {
	return fmt.Sprintf("Sequence:\n\tN: %d\n\tAddress: %s\n", elt.Number, elt.Address)
}
func (elt Command) String() string {
	return fmt.Sprintf("Command:\n\tUniqueRandomTag: %d\n\tCommand: %s\n\t%s\n\tAddress: %s\n", elt.UniqueRandomTag, elt.Command, elt.Sequence.String(), elt.Address)
}
func (slot Slot) String() string {
	return fmt.Sprintf("Slot:\n\tCommand\n\t %v, Decided: %t\n", slot.Command, slot.Decided)
}

// Cmp compares two Sequence Numbers
func (elt Sequence) Cmp(rhs Sequence) int {
	if elt.Number == rhs.Number {
		if elt.Address > rhs.Address {
			return 1
		}
		if elt.Address < rhs.Address {
			return -1
		}
		return 0
	}
	if elt.Number < rhs.Number {
		return -1
	}
	if elt.Number > rhs.Number {
		return 1
	}
	return 0
}

// Eq checks if two commands are the same command
func (elt Command) Eq(rhs Command) bool {
	return elt.Address == rhs.Address && elt.UniqueRandomTag == rhs.UniqueRandomTag
}

// Ping prints a message to whoever got pinged and sends a response back
func (elt *Replica) Ping(_ Request, response *Response) error {
	response.Message = "Successfully Pinged " + elt.Address
	log.Println("I Got Pinged")
	return nil
}

/* <ACCEPTOR ROLE> */

// Prepare ...
func (elt *Replica) Prepare(req Request, resp *Response) error {
	duration := float64(latency)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(duration+offset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(elt.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			log.Fatal("DUMPALL")
		}
	}

	args := req.Prepare
	chatting("")
	chatting("- Prepare:  Args(SLOT: " + strconv.Itoa(args.Slot.Index) + ", SEQUENCE: " + strconv.Itoa(args.Seq.Number) + ")")
	chatting("- Prepare:  " + args.Slot.String())

	if elt.Slots[args.Slot.Index].Sequence.Cmp(args.Seq) == -1 {
		//if elt.Promised < args.Seq
		chatting("- Prepare:  Sequence YES")
		resp.Okay = true
		resp.Promised = args.Seq
		elt.Slots[args.Slot.Index].Sequence = args.Seq
		resp.Command = elt.Slots[args.Slot.Index].Command
	} else {
		resp.Okay = false
		chatting("- Prepare:  Sequence NO. Already promised a higher number: " + strconv.Itoa(elt.Slots[args.Slot.Index].Sequence.Number))
		resp.Promised = elt.Slots[args.Slot.Index].Sequence
	}
	return nil
}

// Accepted ...
func (elt *Replica) Accepted(req Request, resp *Response) error {
	duration := float64(latency)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(duration+offset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(elt.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			log.Fatal("DUMPALL")
		}
	}

	args := req.Accepted
	chatting("")
	chatting("- Accepted:  Args(SLOT: " + strconv.Itoa(args.Slot.Index) + ", SEQUENCE: " + strconv.Itoa(args.Seq.Number) + ")")
	chatting("- Accepted:  " + args.Slot.String())

	if elt.Slots[args.Slot.Index].Sequence.Cmp(args.Seq) == 0 {
		chatting("- Accepted:  Sequence YES")
		resp.Okay = true
		resp.Promised = elt.Slots[args.Slot.Index].Sequence
		elt.ToApply = args.Slot
	} else {
		chatting("- Accepted:  Sequence NO. Already promised a higher number: " + strconv.Itoa(elt.Slots[args.Slot.Index].Sequence.Number))
		resp.Okay = false
		resp.Promised = elt.ToApply.Sequence
	}
	return nil
}

/* </ACCEPTOR ROLE> */

/* <LEARNER ROLE> */

// Decide ...
func (elt *Replica) Decide(req Request, resp *Response) error {
	// Latency sleep
	duration := float64(latency)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(duration+offset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(elt.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			log.Fatal("DUMPALL")
		}
	}

	args := req.Decide
	chatting("")
	chatting("> Decide:  Args(SLOT: " + strconv.Itoa(args.Slot.Index) + ", VALUE: " + args.Value.Command + ")")
	chatting("> Decide:  " + args.Slot.String())

	if elt.Slots[args.Slot.Index].Decided && elt.Slots[args.Slot.Index].Command.Command != args.Value.Command {
		chatting("> Decide:  Already decided slot " + strconv.Itoa(args.Slot.Index) + " with a different command " + args.Value.Command)
		log.Fatal("Decide")
		return nil
	}
	// If already decided, quit
	if elt.Slots[args.Slot.Index].Decided {
		chatting("> Decide:  Already decided slot " + strconv.Itoa(args.Slot.Index) + " with command " + args.Value.Command)
		return nil
	}

	_, ok := elt.Listeners[args.Value.Key]
	if ok {
		elt.Listeners[args.Value.Key] <- args.Value.Command
	}

	command := strings.Split(args.Value.Command, " ")
	args.Slot.Decided = true
	elt.Slots[args.Slot.Index] = args.Slot

	fmt.Println(args.Slot.Index)
	for !allDecided(elt.Slots, args.Slot.Index) {
		time.Sleep(time.Second)
	}
	switch command[0] {
	case "put":
		log.Println("Put " + command[1] + " " + command[2])
		elt.Data[command[1]] = command[2]
		break
	case "get":
		log.Println("Get{" + command[1] + "," + elt.Data[command[1]] + "}")
		break
	case "delete":
		log.Println(command[1] + " deleted.")
		delete(elt.Data, command[1])
		break
	}
	return nil
}

/* </LEARNER ROLE> */

/* <PROPOSER ROLE> */

// Propose ...
func (elt *Replica) Propose(req Request, resp *Response) error {
	lduration := float64(latency)
	loffset := float64(lduration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(lduration+loffset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(elt.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			log.Fatal("ERROR DUMPALL")
		}
	}

	args := req.Propose
	chatting("")
	chatting("* Propose:\n\tArgs(COMMAND: " + args.Command.String() + ")")

	var acceptance Accept
	round := 1

	var pSlot Slot
	pSlot.Sequence = args.Command.Sequence
	if pSlot.Sequence.Number == 0 {
		pSlot.Sequence.Address = elt.Address
		pSlot.Sequence.Number = 1
	}
	pSlot.Command = args.Command
	pSlot.Command.Sequence = pSlot.Sequence

rounds:
	for {
		for i := 1; i < len(elt.Slots); i++ {
			if !elt.Slots[i].Decided {
				pSlot.Index = i
				break
			}
		}
		acceptance.Slot = pSlot

		chatting("* Propose:  Round: " + strconv.Itoa(round))
		chatting("* Propose:  Slot: " + strconv.Itoa(pSlot.Index))
		chatting("* Propose:  N: " + strconv.Itoa(pSlot.Sequence.Number))

		response := make(chan Response, len(elt.Cell))
		for _, v := range elt.Cell {
			go func(v string, slot Slot, sequence Sequence, response chan Response) {
				requ:= Request{Address: elt.Address, Prepare: Prepare{Slot: slot, Seq: sequence}}
				var resp Response
				err := call(v, "Prepare", requ, &resp)
				if err != nil {
					log.Fatal("ERROR PREPARE")
					return
				}
				response <- resp

			}(v, pSlot, pSlot.Sequence, response)
		}

		nTrue := 0
		nFalse := 0
		highestN := 0
		var highestCommand Command
		for numVotes := 0; numVotes < len(elt.Cell); numVotes++ {
			prepareResponse := <-response
			if prepareResponse.Okay {
				nTrue++
			} else {
				nFalse++
			}

			if prepareResponse.Promised.Number > highestN {
				highestN = prepareResponse.Promised.Number
			}
			if prepareResponse.Command.Sequence.Number > highestCommand.Sequence.Number {
				highestCommand = prepareResponse.Command
			}

			if nTrue >= majority(len(elt.Cell)) || nFalse >= majority(len(elt.Cell)) {
				break
			}
		}

		if nTrue >= majority(len(elt.Cell)) {
			acceptance.V = pSlot.Command

			acceptance.N = pSlot.Sequence

			fmt.Println(highestCommand)
			fmt.Println(args.Command)
			if highestCommand.UniqueRandomTag > 0 && highestCommand.UniqueRandomTag != args.Command.UniqueRandomTag {
				acceptance.V = highestCommand
				acceptance.Slot.Command = highestCommand

				req.Accept = acceptance
				call(elt.Address, "Accept", req, resp)
			} else {
				break rounds
			}
		}

		pSlot.Sequence.Number = highestN + 1

		duration := float64(5 * round)
		offset := float64(duration) * rand.Float64()
		time.Sleep(time.Millisecond * time.Duration(duration+offset))
		round++
	}

	req.Accept = acceptance
	call(elt.Address, "Accept", req, resp)

	return nil
}

// Accept ...
func (elt *Replica) Accept(req Request, resp *Response) error {
	// Latency sleep
	lduration := float64(latency)
	loffset := float64(lduration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(lduration+loffset))
	if dumper {
		var daresp Response
		var dareq Request
		err1 := call(elt.Address, "Dumpall", dareq, &daresp)
		if err1 != nil {
			log.Fatal("DUMPALL")
		}
	}

	args := req.Accept
	aSlot := args.Slot
	aN := args.N
	aV := args.V

	chatting("")
	chatting("* Accept:  Args(COMMAND: " + aV.String() + ", SEQUENCE: " + aN.String() + ", SLOT: " + aSlot.String() + ")")

	response := make(chan Response, len(elt.Cell))
	for _, v := range elt.Cell {
		go func(v string, slot Slot, sequence Sequence, command Command, response chan Response) {
			req := Request{Address: elt.Address, Accepted: Accepted{Slot: slot, Seq: sequence, Command: command}}
			var resp Response
			err := call(v, "Accepted", req, &resp)
			if err != nil {
				log.Fatal("ACCEPTED ERROR")
				return
			}
			response <- resp

		}(v, aSlot, aN, aV, response)
	}

	nTrue := 0
	nFalse := 0
	highestN := 0
	for numVotes := 0; numVotes < len(elt.Cell); numVotes++ {
		prepareResponse := <-response
		if prepareResponse.Okay {
			nTrue++
		} else {
			nFalse++
		}

		if prepareResponse.Promised.Number > highestN {
			highestN = prepareResponse.Promised.Number
		}

		if nTrue >= majority(len(elt.Cell)) || nFalse >= majority(len(elt.Cell)) {
			break
		}
	}

	if nTrue >= majority(len(elt.Cell)) {
		chatting("* Accept:  Received enough votes, you can now decide.")
		for _, v := range elt.Cell {
			go func(v string, slot Slot, command Command) {
				req := Request{Address: elt.Address, Decide: Decide{Slot: slot, Value: command}}
				var resp Response
				err := call(v, "Decide", req, &resp)
				if err != nil {
					log.Fatal("Decide (from Accept)")
					return
				}
			}(v, aSlot, aV)
		}

		return nil
	}

	chatting("* Accept:  Not enough votes.  You can't decide. Start over.")
	aV.Sequence.Number = highestN + 1

	duration := float64(5)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Millisecond * time.Duration(duration+offset))

	req1 := Request{Address: elt.Address, Propose: Propose{Command: aV}}
	var resp1 Response
	err := call(elt.Address, "Propose", req1, &resp1)
	if err != nil {
		log.Fatal("Propose")
		return err
	}

	return nil
}

/* <PROPOSER ROLE> */

func printHelp() {
	fmt.Println("\thelp:\n\t\tDisplays a list of recognized commands")
	fmt.Println("\tdumpall:\n\t\tDisplays all info")
	fmt.Println("\tquit:\n\t\tEnds the program")
	fmt.Println("\tput <key> <value>:\n\t\tStores the given <key> and <value>")
	fmt.Println("\tget <key>:\n\t\tRetrieves a value corresponding to the given <key>")
	fmt.Println("\tdelete <key>:\n\t\tFinds a given <key> and its value then deletes both")
}
func main() {
	dumper = false
	if len(os.Args) < 2 {
		log.Fatalf("\t'paxos' must have at least one port number provided as an arguement")
		os.Exit(1)
	}

	chatty = false
	latency = 1
	var myport string
	var ports []string
	var parameter string

	for i, arg := range os.Args {
		if i == 0 {
			continue
		}
		if parameter == "chatty" {
			if arg == "true" {
				chatty = true
			}
			parameter = ""
		} else if parameter == "latency" {
			latency, _ = strconv.Atoi(arg)
			parameter = ""
		} else if strings.HasPrefix(arg, "-") {
			if arg == "-v" {
				parameter = "chatty"
			} else if arg == "-l" {
				parameter = "latency"
			} else {
				log.Fatalf("\t-%s is not a recognized parameter for 'paxos'", arg)
				os.Exit(1)
			}
		} else if myport == "" {
			myport = arg
		} else {
			ports = append(ports, arg)
		}
	}

	node := &Replica{
		Address:   myport,
		Cell:      append(ports, myport),
		Slots:     make([]Slot, 100),
		Listeners: make(map[string]chan string),
		Data:      make(map[string]string),
	}
	rpc.Register(node)
	rpc.HandleHTTP()

	go func() {
		err := http.ListenAndServe(getAddress(myport), nil)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		commands := strings.Split(scanner.Text(), " ")

		switch commands[0] {
		case "help":
			printHelp()
			break
		case "dumpall":
			for _, v := range node.Cell {
				var resp Response
				var req Request
				err := call(v, "Dumpall", req, &resp)
				if err != nil {
					log.Fatal("DUMPALL")
					continue
				}
			}
			break
		case "quit":
			fmt.Println("Shutting down")
			os.Exit(1)
			break
		case "get":
			var command Command
			command.Command = strings.Join(commands, " ")
			command.Address = node.Address
			// Assign the command a tag
			command.UniqueRandomTag, _ = strconv.ParseInt(strconv.FormatInt(time.Now().Unix(), 10)+command.Address, 10, 64)
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := strconv.FormatInt(command.UniqueRandomTag, 10) + command.Address
			command.Key = key
			node.Listeners[key] = respChan

			req := Request{Address: node.Address, Propose: Propose{Command: command}}
			var resp Response
			err := call(node.Address, "Propose", req, &resp)
			if err != nil {
				log.Fatal("Propose")
				continue
			}

			go func() {
				chatting("Finished " + <-node.Listeners[key])
			}()
			break
		case "put":
			var command Command
			command.Command = strings.Join(commands, " ")
			command.Address = node.Address
			// Assign the command a tag
			command.UniqueRandomTag, _ = strconv.ParseInt(strconv.FormatInt(time.Now().Unix(), 10)+command.Address, 10, 64)
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := strconv.FormatInt(command.UniqueRandomTag, 10) + command.Address
			command.Key = key
			node.Listeners[key] = respChan

			req := Request{Address: node.Address, Propose: Propose{Command: command}}
			var resp Response
			err := call(node.Address, "Propose", req, &resp)
			if err != nil {
				log.Fatal("Propose")
				continue
			}

			go func() {
				chatting("Finished " + <-node.Listeners[key])
			}()
			break
		case "delete":
			var command Command
			command.Command = strings.Join(commands, " ")
			command.Address = node.Address
			// Assign the command a tag
			command.UniqueRandomTag, _ = strconv.ParseInt(strconv.FormatInt(time.Now().Unix(), 10)+command.Address, 10, 64)
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := strconv.FormatInt(command.UniqueRandomTag, 10) + command.Address
			command.Key = key
			node.Listeners[key] = respChan

			req := Request{Address: node.Address, Propose: Propose{Command: command}}
			var resp Response
			err := call(node.Address, "Propose", req, &resp)
			if err != nil {
				log.Fatal("Propose")
				continue
			}

			go func() {
				chatting("Finished " + <-node.Listeners[key])
			}()
			break
		default:
			printHelp()
			break
		}
	}
}
