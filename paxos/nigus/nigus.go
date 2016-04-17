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
	Sequence Sequence // most recently promised sequence number
	Command  Command  // Most recently accepted command
	Decided  bool     // When it was decided
	Index    int
}

// Command ...
type Command struct {
	Address  string // The Proposer
	Sequence Sequence
	Command  string
	Tag      int64 // To uniquely identify a command
	Key      string
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
	Slot    Slot
	Seq     Sequence
	Command Command
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

/*
 * HELPERS
 */

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
	log.Println("====================================")
	log.Println("Database")
	for k, v := range elt.Data {
		log.Printf("    Key: %s, Value: %s", k, v)
	}
	log.Println("Slots")
	for k, v := range elt.Slots {
		if v.Decided {
			log.Printf("    [%d] %v", k, v)
		}
	}
	log.Println("====================================")
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

func failure(f string) {
	log.Println("Call", f, "has failed.")
}

func help() {
	fmt.Println("==============================================================")
	fmt.Println("                          COMMANDS")
	fmt.Println("==============================================================")
	fmt.Println("help               - Display this message.")
	fmt.Println("dump               - Display info about the current node.")
	fmt.Println("put <key> <value>  - Put a value.")
	fmt.Println("get <key>          - Get a value.")
	fmt.Println("delete <key>       - Delete a value.")
	fmt.Println("quit               - Quit the program.")
	fmt.Println("==============================================================")
}

func majority(size int) int {
	return int(math.Ceil(float64(size) / 2))
}

func readLine(readline chan string) {
	// Get command
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("READLINE ERROR:", err)
	}
	readline <- line
}

func chatting(message string) {
	if chatty {
		log.Println(message)
	}
}

// String converts to string and prints contents of struct
func (elt Sequence) String() string {
	return fmt.Sprintf("Sequence{N: %d, Address: %s}", elt.Number, elt.Address)
}
func (elt Command) String() string {
	return fmt.Sprintf("Command{Tag: %d, Command: %s, Sequence: %s, Address: %s}", elt.Tag, elt.Command, elt.Sequence.String(), elt.Address)
}
func (slot Slot) String() string {
	return fmt.Sprintf("Slot{Command: %v, Decided: %t}", slot.Command, slot.Decided)
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
	return elt.Address == rhs.Address && elt.Tag == rhs.Tag
}

// Ping prints a message to whoever got pinged and sends a response back
func (elt *Replica) Ping(_ Request, response *Response) error {
	response.Message = "Successfully Pinged " + elt.Address
	log.Println("I Got Pinged")
	return nil
}

/*
 *	ACCEPTOR
 */

//Prepare (slot, seq) -> (okay, promised, command)
//  A prepare request asks the replica to promise not to accept any future
//	prepare or accept messages for slot slot unless they have a higher sequence number than seq.
//
//	If this is the highest seq number it has been asked to promise,
//		it should return True for okay,
//		seq as the new value for promised (which it must also store),
//		and whatever command it most recently accepted (using the Accept operation below).
//	If it has already promised a number >= seq,
//		it should reject the request by returning False for the okay result value.
//		In this case it should also return the highest seq it has promised as the return value promised, and the command return value can be ignored.
func (elt *Replica) Prepare(req Request, resp *Response) error {
	// Latency sleep
	duration := float64(latency)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(duration+offset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(elt.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////

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

//Accepted (slot, seq, command) -> (okay, promised)
//	An accept request asks the replica to accept the value command for slot slot,
//	but only if the replica has not promised a value greater than seq for this slot
//	(less-than or equal-to is okay, and it is okay if no seq value has ever been promised for this slot).
//
//	If successful,
//		the command should be stored in the slot as the most-recently-accepted value.
//		okay should be True, and promised is the last value promised for this slot.
//
//	If the request fails because a higher value than seq has been promised for this slot,
//		okay should be False and promised should be the last value promised for this slot.
func (elt *Replica) Accepted(req Request, resp *Response) error {
	// Latency sleep
	duration := float64(latency)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(duration+offset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(elt.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////

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
		//resp.Promised = elt.Slots[args.Slot.Index].Sequence
		resp.Promised = elt.ToApply.Sequence
	}
	return nil
}

/*
 *	LEARNER
 */

//Decide (s, v)
//	A decide request indicates that another replica has learned of the decision for this slot.
//	Since we trust other hosts in the cell, we accept the value.
//	It would be good to check if you have already been notified of a decision, and if that decision contradicts this one.
//	In that case, there is an error somewhere and a panic is appropriate.
//
//	If this is the first time that this replica has learned about the decision for this slot,
//	it should also check if it (and possibly slots after it) can now be applied.
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
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////

	args := req.Decide
	chatting("")
	chatting("> Decide:  Args(SLOT: " + strconv.Itoa(args.Slot.Index) + ", VALUE: " + args.Value.Command + ")")
	chatting("> Decide:  " + args.Slot.String())

	if elt.Slots[args.Slot.Index].Decided && elt.Slots[args.Slot.Index].Command.Command != args.Value.Command {
		chatting("> Decide:  Already decided slot " + strconv.Itoa(args.Slot.Index) + " with a different command " + args.Value.Command)
		failure("Decide")
		return nil
	}
	// If already decided, quit
	if elt.Slots[args.Slot.Index].Decided {
		chatting("> Decide:  Already decided slot " + strconv.Itoa(args.Slot.Index) + " with command " + args.Value.Command)
		return nil
	}

	_, ok := elt.Listeners[args.Value.Key]
	if ok {
		// if found send the result across the channel, then remove the channel from the map and throw it away
		elt.Listeners[args.Value.Key] <- args.Value.Command
	}

	command := strings.Split(args.Value.Command, " ")
	//elt.Slots[args.Slot.Index] = Slot{Command: args.Value, Decided: true}
	args.Slot.Decided = true
	elt.Slots[args.Slot.Index] = args.Slot

	// TODO: If this is the first time that this replica has learned about the decision for this slot,
	// it should also check if it (and possibly slots after it) can now be applied.
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
		log.Println("Get - Key: " + command[1] + ", Value: " + elt.Data[command[1]])
		break
	case "delete":
		log.Println(command[1] + " deleted.")
		delete(elt.Data, command[1])
		break
	}
	return nil
}

/*
 *	PROPOSER
 */

// Propose ...
func (elt *Replica) Propose(req Request, resp *Response) error {
	// Latency sleep
	lduration := float64(latency)
	loffset := float64(lduration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(lduration+loffset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(elt.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////

	args := req.Propose
	chatting("")
	chatting("* Propose:  Args(COMMAND: " + args.Command.String() + ")")

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
		// Build the slot for the first undecided slot in Slots
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

		// Send a Prepare message out to the entire cell (using a go routine per replica)
		response := make(chan Response, len(elt.Cell))
		for _, v := range elt.Cell {
			go func(v string, slot Slot, sequence Sequence, response chan Response) {
				req := Request{Address: elt.Address, Prepare: Prepare{Slot: slot, Seq: sequence}}
				var resp Response
				err := call(v, "Prepare", req, &resp)
				if err != nil {
					failure("Prepare (from Propose)")
					return
				}
				// Send the response over a channel, we can assume that a majority WILL respond
				response <- resp

			}(v, pSlot, pSlot.Sequence, response)
		}

		// Get responses from go routines
		numYes := 0
		numNo := 0
		highestN := 0
		var highestCommand Command
		for numVotes := 0; numVotes < len(elt.Cell); numVotes++ {
			// pull from the channel response
			prepareResponse := <-response
			if prepareResponse.Okay {
				numYes++
			} else {
				numNo++
			}

			// make note of the highest n value that any replica returns to you
			if prepareResponse.Promised.Number > highestN {
				highestN = prepareResponse.Promised.Number
			}
			// track the highest-sequenced command that has already been accepted by one or more of the replicas
			if prepareResponse.Command.Sequence.Number > highestCommand.Sequence.Number {
				highestCommand = prepareResponse.Command
			}

			// If I have a majority
			if numYes >= majority(len(elt.Cell)) || numNo >= majority(len(elt.Cell)) {
				break
			}
		}

		// If I have a majority
		if numYes >= majority(len(elt.Cell)) {
			// select your value
			// If none of the replicas that voted for you included a value, you can pick your own.
			acceptance.V = pSlot.Command

			// In either case, you should associate the value you are about to send out for acceptance with your promised n
			acceptance.N = pSlot.Sequence

			// If one or more of those replicas that voted for you have already accepted a value, KEY WORD IS ACCEPTED
			// you should pick the highest-numbered value from among them, i.e. the one that was accepted with the highest n value.
			fmt.Println(highestCommand)
			fmt.Println(args.Command)
			if highestCommand.Tag > 0 && highestCommand.Tag != args.Command.Tag {
				acceptance.V = highestCommand
				acceptance.Slot.Command = highestCommand

				req.Accept = acceptance
				call(elt.Address, "Accept", req, resp)
			} else {
				break rounds
			}
		}

		// If I don't get a majority
		// Generate a larger n for the next round
		pSlot.Sequence.Number = highestN + 1

		// To pause, pick a random amount of time between, say, 5ms and 10ms. If you fail again, pick a random sleep time between 10ms and 20ms
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
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////

	args := req.Accept
	aSlot := args.Slot
	aN := args.N
	aV := args.V

	chatting("")
	chatting("* Accept:  Args(COMMAND: " + aV.String() + ", SEQUENCE: " + aN.String() + ", SLOT: " + aSlot.String() + ")")

	// Send an accept request to all replicas and gather the results
	response := make(chan Response, len(elt.Cell))
	for _, v := range elt.Cell {
		go func(v string, slot Slot, sequence Sequence, command Command, response chan Response) {
			req := Request{Address: elt.Address, Accepted: Accepted{Slot: slot, Seq: sequence, Command: command}}
			var resp Response
			err := call(v, "Accepted", req, &resp)
			if err != nil {
				failure("Accepted (from Accept)")
				return
			}
			// Send the response over a channel, we can assume that a majority WILL respond
			response <- resp

		}(v, aSlot, aN, aV, response)
	}

	// Get responses from go routines
	numYes := 0
	numNo := 0
	highestN := 0
	for numVotes := 0; numVotes < len(elt.Cell); numVotes++ {
		// pull from the channel response
		prepareResponse := <-response
		//resp{Command, Promised, Okay}
		if prepareResponse.Okay {
			numYes++
		} else {
			numNo++
		}

		// make note of the highest n value that any replica returns to you
		if prepareResponse.Promised.Number > highestN {
			highestN = prepareResponse.Promised.Number
		}

		// If I have a majority
		if numYes >= majority(len(elt.Cell)) || numNo >= majority(len(elt.Cell)) {
			break
		}
	}

	if numYes >= majority(len(elt.Cell)) {
		chatting("* Accept:  Received enough votes, you can now decide.")
		for _, v := range elt.Cell {
			go func(v string, slot Slot, command Command) {
				req := Request{Address: elt.Address, Decide: Decide{Slot: slot, Value: command}}
				var resp Response
				err := call(v, "Decide", req, &resp)
				if err != nil {
					failure("Decide (from Accept)")
					return
				}
			}(v, aSlot, aV)
		}

		return nil
	}

	chatting("* Accept:  Not enough votes.  You can't decide. Start over.")
	aV.Sequence.Number = highestN + 1

	// To pause, pick a random amount of time between, say, 5ms and 10ms. If you fail again, pick a random sleep time between 10ms and 20ms
	duration := float64(5)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Millisecond * time.Duration(duration+offset))

	req1 := Request{Address: elt.Address, Propose: Propose{Command: aV}}
	var resp1 Response
	err := call(elt.Address, "Propose", req1, &resp1)
	if err != nil {
		failure("Propose")
		return err
	}

	return nil
}
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
					failure("DUMPALL")
					continue
				}
			}
			break
		case "quit":
			fmt.Println("QUIT")
			fmt.Println("Goodbye. . .")
			break
		case "get":
			var command Command
			command.Command = strings.Join(commands, " ")
			command.Address = node.Address
			// Assign the command a tag
			command.Tag, _ = strconv.ParseInt(strconv.FormatInt(time.Now().Unix(), 10)+command.Address, 10, 64)
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := strconv.FormatInt(command.Tag, 10) + command.Address
			command.Key = key
			node.Listeners[key] = respChan

			req := Request{Address: node.Address, Propose: Propose{Command: command}}
			var resp Response
			err := call(node.Address, "Propose", req, &resp)
			if err != nil {
				failure("Propose")
				continue
			}

			go func() {
				chatting("DONE: " + <-node.Listeners[key])
			}()
			break
		case "put":
			var command Command
			command.Command = strings.Join(commands, " ")
			command.Address = node.Address
			// Assign the command a tag
			command.Tag, _ = strconv.ParseInt(strconv.FormatInt(time.Now().Unix(), 10)+command.Address, 10, 64)
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := strconv.FormatInt(command.Tag, 10) + command.Address
			command.Key = key
			node.Listeners[key] = respChan

			req := Request{Address: node.Address, Propose: Propose{Command: command}}
			var resp Response
			err := call(node.Address, "Propose", req, &resp)
			if err != nil {
				failure("Propose")
				continue
			}

			go func() {
				chatting("DONE: " + <-node.Listeners[key])
			}()
			break
		case "delete":
			var command Command
			command.Command = strings.Join(commands, " ")
			command.Address = node.Address
			// Assign the command a tag
			command.Tag, _ = strconv.ParseInt(strconv.FormatInt(time.Now().Unix(), 10)+command.Address, 10, 64)
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := strconv.FormatInt(command.Tag, 10) + command.Address
			command.Key = key
			node.Listeners[key] = respChan

			req := Request{Address: node.Address, Propose: Propose{Command: command}}
			var resp Response
			err := call(node.Address, "Propose", req, &resp)
			if err != nil {
				failure("Propose")
				continue
			}

			go func() {
				chatting("DONE: " + <-node.Listeners[key])
			}()
			break
		default:
			printHelp()
			break
		}
	}
}
