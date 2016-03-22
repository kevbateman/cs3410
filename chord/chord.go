package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Nothing is nothing
type Nothing struct{}

// Key is a string that is just used for distinction between other string types
type Key string

// Value is a string that is just used for distinction between other string types
type Value string

// Node holds all node data
type Node struct {
	Address     string
	Predecessor string
	Successors  [3]string
	Bucket      map[Key]Value
	Fingers     []string
	Next        int
	mutex       sync.Mutex
}

// KeyValue holds a key and a value
type KeyValue struct {
	Key   Key
	Value Value
}

// KeyFound holds a key and a value specifing if it has been found
type KeyFound struct {
	Address string
	Found   bool
}

/*START OF RUSS HELP CODE*/
func hashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

const keySize = sha1.Size * 8

var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(keySize), nil)

func (elt *Node) jump(fingerentry int) *big.Int {
	n := hashString(elt.Address)
	two := big.NewInt(2)
	fingerentryminus1 := big.NewInt(int64(fingerentry) - 1)
	jump := new(big.Int).Exp(two, fingerentryminus1, nil)
	sum := new(big.Int).Add(n, jump)

	return new(big.Int).Mod(sum, hashMod)
}

func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	}
	return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
}
func getLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
} /*END OF RUSS HELP CODE*/

func call(address string, method string, request interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		//log.Printf("\tCommand(%s) Error connecting to server at %s: %v", method, address, err)
		return fmt.Errorf("\tCommand(%s) Error connecting to server at %s: %v", method, address, err)
	}
	defer client.Close()
	return client.Call("Node."+method, request, reply)
}

func (elt *Node) find(key string) string {
	keyfound := KeyFound{elt.Successors[0], false}
	count := 32
	for !keyfound.Found {
		if count > 0 {
			//log.Printf("find is calling FindSuccessor")
			err := call(keyfound.Address, "FindSuccessor", hashString(key), &keyfound)
			if err == nil {
				count--
			} else {
				count = 0
			}
		} else {
			return ""
		}
	}
	return keyfound.Address
}

func (elt *Node) findHash(key *big.Int) string {
	keyfound := KeyFound{elt.Successors[0], false}
	count := 32
	for !keyfound.Found {
		if count > 0 {
			//log.Printf("findHash is calling FindSuccessor")
			err := call(keyfound.Address, "FindSuccessor", key, &keyfound)
			if err == nil {
				count--
			} else {
				count = 0
			}
		} else {
			return ""
		}
	}
	return keyfound.Address
}

// CheckPredecssor checks if predecessor has failed
func (elt *Node) checkPredecessor() error {
	if elt.Predecessor != "" {
		client, err := rpc.DialHTTP("tcp", elt.Predecessor)
		if err != nil {
			log.Printf("\tPredecessor '%s' has failed", elt.Predecessor)
			elt.Predecessor = ""
			//elt.Successors[0] = elt.Address
		} else {
			client.Close()
		}
	}
	return nil
}

// FixFingers refreshes finger table enteries
func (elt *Node) fixFingers() error {
	elt.Next++
	if elt.Next > len(elt.Fingers)-1 {
		elt.Next = 0
	}
	//log.Printf("fixFingers is calling findHash")
	addrs := elt.findHash(elt.jump(elt.Next))

	if elt.Fingers[elt.Next] != addrs && addrs != "" {
		log.Printf("\tWriting FingerTable entry '%d' as '%s'\n", elt.Next, addrs)
		elt.Fingers[elt.Next] = addrs
	}
	for {
		elt.Next++
		if elt.Next > len(elt.Fingers)-1 {
			elt.Next = 0
			return nil
		}

		if between(hashString(elt.Address), elt.jump(elt.Next), hashString(addrs), false) && addrs != "" {
			elt.Fingers[elt.Next] = addrs
		} else {
			elt.Next--
			return nil
		}
	}
}

// ClosestPrecedingNode finds closest preceding node
func (elt *Node) closestPrecedingNode(id *big.Int) string {
	for i := len(elt.Fingers) - 1; i > 0; i-- {
		if between(hashString(elt.Address), hashString(elt.Fingers[i]), id, false) {
			return elt.Fingers[i]
		}
	}
	return elt.Successors[0]
}

// FindSuccessor asks Node elt to find the successor of address
func (elt *Node) FindSuccessor(hash *big.Int, keyfound *KeyFound) error {
	if between(hashString(elt.Address), hash, hashString(elt.Successors[0]), true) {
		keyfound.Address = elt.Successors[0]
		keyfound.Found = true
		return nil
	}
	keyfound.Address = elt.closestPrecedingNode(hash)
	return nil
}

// Notify tells the node at 'address' that it might be our predecessor
func (elt *Node) Notify(address string, empty *struct{}) error {
	if elt.Predecessor == "" ||
		between(hashString(elt.Predecessor),
			hashString(address),
			hashString(elt.Address),
			false) {
		elt.Predecessor = address
	}
	return nil
}

// Stabilize is called every second
func (elt *Node) stabilize() error {
	var successors []string
	err := call(elt.Successors[0], "GetSuccessorList", &struct{}{}, &successors)
	if err == nil {
		elt.Successors[1] = successors[0]
		elt.Successors[2] = successors[1]
	} else {
		log.Printf("\tPrimary successor '%s' failed", elt.Successors[0])
		if elt.Successors[0] == "" {
			log.Printf("\tSetting primary successor to address of this node '%s'", elt.Address)
			elt.Successors[0] = elt.Address
		} else {
			log.Printf("\tSetting secondary successor '%s' as primary ", elt.Successors[1])
			elt.Successors[0] = elt.Successors[1]
			elt.Successors[1] = elt.Successors[2]
			elt.Successors[2] = ""
		}
	}

	x := ""
	call(elt.Successors[0], "GetPredecessor", &struct{}{}, &x)

	if between(hashString(elt.Address),
		hashString(x),
		hashString(elt.Successors[0]),
		false) && x != "" {
		log.Printf("\tSetting primary successor to '%s'", x)
		elt.Successors[0] = x
	}

	err = call(elt.Successors[0], "Notify", elt.Address, &struct{}{})
	if err != nil {
	}
	return nil
}

// GetPredecessor simple returns the predecessor of node
func (elt *Node) GetPredecessor(empty1 *struct{}, predecessor *string) error {
	*predecessor = elt.Predecessor
	return nil
}

// GetSuccessorList simple returns the predecessor of node
func (elt *Node) GetSuccessorList(empty1 *struct{}, successors *[]string) error {
	*successors = elt.Successors[:]
	return nil
}

// Join an existing ring
func (elt *Node) Join(address string, successor *string) error {
	*successor = elt.find(address)
	call(*successor, "GetAll", address, &struct{}{})
	return nil
}

// Dump lists the key/value pairs stored locally.
func (elt *Node) Dump(empty1 *struct{}, info *Node) error {
	info.Address = elt.Address
	info.Predecessor = elt.Predecessor
	info.Successors = elt.Successors
	info.Bucket = elt.Bucket
	var old string
	for i := 0; i < len(elt.Fingers); i++ {
		if old != elt.Fingers[i] {
			info.Fingers = append(info.Fingers, strconv.Itoa(i)+":\t", elt.Fingers[i], "\n\t\t\t")
			old = elt.Fingers[i]
		}
	}
	return nil
}

// PutAll inserts the given key and value into the currently active ring.
func (elt *Node) PutAll(bucket map[Key]Value, empty *struct{}) error {
	for key, value := range bucket {
		elt.Bucket[key] = value
	}
	return nil
}

// GetAll inserts the given key and value into the currently active ring.
func (elt *Node) GetAll(address string, empty *struct{}) error {
	tempBucket := make(map[Key]Value)
	for key, value := range elt.Bucket {
		if between(hashString(elt.Predecessor), hashString(string(key)), hashString(address), false) {
			tempBucket[key] = value
			delete(elt.Bucket, key)
		}
	}
	call(address, "PutAll", tempBucket, &struct{}{})
	return nil
}

func (elt *Node) handleData(command string, keyvalue *KeyValue) error {
	if command == "Get" {
		// log.Printf("handling get data")
		return call(elt.find(string(keyvalue.Key)), command, keyvalue.Key, &keyvalue.Value)
	}
	return call(elt.find(string(keyvalue.Key)), command, keyvalue, &struct{}{})
}

// Put inserts the given key and value into the currently active ring.
func (elt *Node) Put(keyvalue *KeyValue, empty *struct{}) error {
	elt.Bucket[keyvalue.Key] = keyvalue.Value
	log.Printf("\t%s was added to this node", *keyvalue)
	return nil
}

// Get retrieves the a value stored in the ring by a given key.
func (elt *Node) Get(key Key, value *Value) error {
	// log.Printf("getting data from %s", elt.Address)
	if val, ok := elt.Bucket[key]; ok {
		*value = val
		log.Printf("\t{%s %s} value was retrieved from this node", key, val)
		return nil
	}
	return fmt.Errorf("\tKey '%s' does not exist in ring", key)
}

// Delete inserts the given key and value into the currently active ring.
func (elt *Node) Delete(keyvalue *KeyValue, empty *struct{}) error {
	if value, ok := elt.Bucket[keyvalue.Key]; ok {
		delete(elt.Bucket, keyvalue.Key)
		log.Printf("\t{%s %s} was removed from this node", keyvalue.Key, value)
		return nil
	}
	return fmt.Errorf("\tKey '%s' does not exist in ring", keyvalue.Key)
}

// Ping is used to ping between server and node
func (elt *Node) Ping(address string, pong *bool) error {
	if address == "SUCCESS" {
		log.Printf("\tSUCCESS: PINGED '%s'", elt.Address)
		*pong = true
		return nil
	}
	return call(address, "Ping", "SUCCESS", pong)
}

func printHelp() {
	fmt.Println("\thelp:\n\t\tDisplays a list of recognized commands")
	fmt.Println("\tquit:\n\t\tEnds the program")
	fmt.Println("\tport <n>:\n\t\tSet listening port to <n>. Default is ':3410'")
	fmt.Println("\tcreate:\n\t\tCreate a new ring on the current port")
	fmt.Println("\tget <key>:\n\t\tGet the value from the current node corresponding to the given <key>")
	fmt.Println("\tput <key> <value>:\n\t\tGet the value from the current node corresponding to the given key")
}
func create(node *Node, port string) error {
	go func() {
		rpc.Register(node)
		rpc.HandleHTTP()
		log.Fatal(http.ListenAndServe(port, nil), "")
	}()
	time.Sleep(time.Second)
	return nil
}

//
// MAIN FUNCTION STARTS HERE
//
func main() {
	fmt.Println()
	if len(os.Args) != 1 {
		log.Fatalf("\t'chord' takes no arguemnts")
	}

	port := ":3410"
	active := false

	node := Node{
		Address:     getLocalAddress() + port,
		Predecessor: "",
		Successors:  [3]string{getLocalAddress() + port},
		Bucket:      make(map[Key]Value),
		Fingers:     make([]string, 161),
		Next:        0,
	}
	go func() {
		for {
			if active {
				node.checkPredecessor()
				node.stabilize()
				node.fixFingers()
			}
			time.Sleep(time.Second)
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Default port is ':3410'\nEnter command:")
	for scanner.Scan() {
		commands := strings.Split(scanner.Text(), " ")
		switch commands[0] {
		case "help":
			printHelp()
		case "quit", "clear", "exit":
			log.Printf("\tPutting all data in successor '%s'", node.Successors[0])
			call(node.Successors[0], "PutAll", node.Bucket, &struct{}{})
			log.Printf("\tShutting down\n\n")
			os.Exit(0)
		case "port":
			if active {
				log.Printf("\tFAILED: Cannot change port after creating or joining")
			} else if len(commands) == 2 {
				port = commands[1]
				node.Address = getLocalAddress() + port
			}
			log.Printf("\tPort is currently set to '%s'", port)
		case "create":
			if !active {
				create(&node, port)
				log.Printf("\tCreated ring at '%s'", node.Address)
				active = true
			} else {
				log.Printf("\tFAILED: A ring has already been created or joined")
			}
		case "ping":
			if active {
				if len(commands) == 2 {
					pong := false
					call(node.Address, "Ping", commands[1], &pong)
					if pong {
						log.Printf("\tSUCCESS: PINGED '%s'", commands[1])
					} else {
						log.Printf("\tFAILED: Could not PING '%s'", commands[1])
					}
				} else if len(commands) == 1 {
					log.Printf("\tFAILED: Missing <address> parameter; use 'ping <address>' command: ")
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'ping <address>' command")
				}
			} else {
				log.Printf("\tFAILED: Node must be active in order to 'ping <address>'")
			}
		case "get":
			if active {
				if len(commands) == 2 {
					keyvalue := KeyValue{Key(commands[1]), Value("")}
					err := node.handleData("Get", &keyvalue)
					if err == nil {
						log.Printf("\tSUCCESS: Key '%s' retrieved value '%s' from ring", keyvalue.Key, keyvalue.Value)
					} else {
						log.Printf("\tFAILED: Key '%s' not found in ring. ERROR: %s", commands[1], err)
					}
				} else if len(commands) == 1 {
					log.Printf("\tFAILED: Missing <key> parameter; use 'get <key>' command: ")
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'get <key>' command")
				}
			} else {
				log.Printf("\tFAILED: Node must be active in order to 'get <key>' values")
			}
		case "put":
			if active {
				if len(commands) == 3 {
					keyvalue := KeyValue{Key(commands[1]), Value(commands[2])}
					err := node.handleData("Put", &keyvalue)
					if err == nil {
						log.Printf("\tSUCCESS: %s inserted into ring", keyvalue)
					} else {
						log.Printf("\tFAILED: %s not added to ring", keyvalue)
					}
				} else if len(commands) == 2 {
					log.Printf("\tFAILED: Missing <value> parameter; use 'put <key> <value>' command: ")
				} else if len(commands) == 1 {
					log.Printf("\tFAILED: Missing <key> and <value> parameters; use 'put <key> <value>' command")
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'put <key> <value>' command")
				}
			} else {
				log.Printf("\tFAILED: Node must be active in order to 'put <key> <value>' pairs")
			}
		case "delete":
			if active {
				if len(commands) == 2 {
					keyvalue := KeyValue{Key(commands[1]), Value("")}
					err := node.handleData("Delete", &keyvalue)
					if err == nil {
						log.Printf("\tSUCCESS: Key '%s' removed from ring", commands[1])
					} else {
						log.Printf("\tFAILED: Key '%s' not found in ring", commands[1])
					}
				} else if len(commands) == 1 {
					log.Printf("\tFAILED: Missing <key> parameter; use 'delete <key>' command: ")
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'delete <key>' command")
				}
			} else {
				log.Printf("\tFAILED: Node must be active to 'delete <key>' values")
			}
		case "dump":
			if active {
				if len(commands) == 1 {
					var values Node
					err := call(node.Address, "Dump", &struct{}{}, &values)
					if err == nil {
						log.Printf("\n\tAddress:\t%s\n\tPredecessor:\t%s\n\tSuccessors:\t%v\n\tBucket:\t\t%v\n\tFingers:\t%v",
							values.Address, values.Predecessor, values.Successors, values.Bucket, values.Fingers)
					}
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'dump' command")
				}
			} else {
				log.Printf("\tFAILED: Node must be active to 'dump' values")
			}
		case "join":
			if !active {
				if len(commands) == 2 {
					create(&node, port)
					active = true
					var successor string
					err := call(commands[1], "Join", node.Address, &successor)
					if err == nil {
						log.Printf("\tSetting successor to '%s'", successor)
						node.Successors[0] = successor
					} else {
						log.Fatalf("\tFAILED: Could not join ring at '%s'", successor)
					}
				} else if len(commands) == 1 {
					log.Printf("\tFAILED: Missing <address> parameter; use 'join <address>' command: ")
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'join <address>' command")
				}
			} else {
				log.Printf("\tFAILED: A ring has already been joined or created")
			}
		case "lookup":
			if active {
				if len(commands) == 2 {
					var value string
					err := call(node.Address, "Get", commands[1], &value)
					if err == nil {
						if len(value) > 0 {
							log.Printf("\tSUCCESS: Retrieved value '%s'", value)
						} else {
							log.Printf("\tFAILED: Could not retrieve a value with key '%s'", commands[1])
						}
					}
				} else if len(commands) == 1 {
					log.Printf("\tFAILED: Missing <key> parameter; use 'get <key>' command: ")
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'get <key>' command")
				}
			} else {
				log.Printf("\tFAILED: Node must be active in order to 'get <key>' values")
			}
		default:
			log.Printf("\tUnrecognized Command '%s'", scanner.Text())
		}
	}
}
