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
	Fingers     [161]string
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
	defer client.Close()
	if err != nil {
		client.Close()
		log.Printf("\tError connecting to server at %s: %v", address, err)
		return nil
	}
	return client.Call("Node."+method, request, reply)
}

func (elt *Node) find(key string) string {
	keyfound := KeyFound{elt.Successors[0], false}
	count := 32
	for !keyfound.Found {
		if count > 0 {
			call(keyfound.Address, "FindSuccessor", hashString(key), &keyfound)
			count--
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
			call(keyfound.Address, "FindSuccessor", key, &keyfound)
			count--
		} else {
			return ""
		}
	}
	return keyfound.Address
}

// CheckPredecssor checks if predecessor has failed
func (elt *Node) checkPredecessor() error {
	client, err := rpc.DialHTTP("tcp", elt.Predecessor)
	defer client.Close()
	if err != nil {
		elt.Predecessor = ""
	}
	return nil
}

// FixFingers refreshes finger table enteries
func (elt *Node) fixFingers() error {
	elt.Next = (elt.Next + 1) % len(elt.Fingers)
	elt.Fingers[elt.Next] = elt.findHash(elt.jump(elt.Next))
	return nil
}

// FindSuccessor asks Node elt to find the successor of address
func (elt *Node) FindSuccessor(hash *big.Int, keyfound *KeyFound) error {
	if between(hashString(elt.Address), hash, hashString(elt.Successors[0]), true) {
		keyfound.Address = elt.Successors[0]
		keyfound.Found = true
	} else {
		keyfound.Address = elt.Successors[0]
	}
	return nil
}

// Put inserts the given key and value into the currently active ring.
func (elt *Node) Put(keyvalue *KeyValue, empty *struct{}) error {
	if between(hashString(elt.Address), hashString(string(keyvalue.Key)), hashString(elt.Successors[0]), true) {
		elt.Bucket[keyvalue.Key] = keyvalue.Value
	} else {
		call(elt.find(string(keyvalue.Key)), "Put", keyvalue, &struct{}{})
	}
	return nil
}

// Notify tells the node at 'address' that it might be our predecessor
func (elt *Node) Notify(address string, empty *struct{}) error {
	if elt.Predecessor == "" ||
		between(hashString(elt.Predecessor),
			hashString(address),
			hashString(elt.Address),
			false) {
		log.Printf("\tSetting predecessor to '%s'", address)
		elt.Predecessor = address
	}
	return nil
}

// Stabilize is called every second
func (elt *Node) stabilize() error {
	x := ""
	call(elt.Successors[0], "GetPredecessor", &struct{}{}, &x)
	if between(hashString(elt.Address),
		hashString(x),
		hashString(elt.Successors[0]),
		false) && x != "" {
		log.Printf("\tSetting successor to '%s'", x)
		elt.Successors[0] = x
	}
	call(elt.Successors[0], "Notify", elt.Address, &struct{}{})
	//call(elt.Address, "CheckPredecssor", &struct{}{}, &struct{}{})
	return nil
}

// GetPredecessor simple returns the predecessor of node
func (elt *Node) GetPredecessor(empty1 *struct{}, predecessor *string) error {
	*predecessor = elt.Predecessor
	return nil
}

// Join an existing ring
func (elt *Node) Join(address string, successor *string) error {
	*successor = elt.find(address)
	return nil
}

// Dump lists the key/value pairs stored locally.
func (elt *Node) Dump(empty1 *struct{}, info *Node) error {
	info.Address = elt.Address
	info.Predecessor = elt.Predecessor
	info.Successors[0] = elt.Successors[0]
	info.Bucket = elt.Bucket
	return nil
}

// Delete has the peer remove the given key-value from the currently active ring.
func (elt *Node) Delete(key Key, empty *struct{}) error {
	if between(hashString(elt.Address), hashString(string(key)), hashString(elt.Successors[0]), true) {
		delete(elt.Bucket, key)
	} else {
		call(elt.find(string(key)), "Delete", key, &struct{}{})
	}
	return nil
}

// Get finds the given key-value in the currently active ring.
func (elt *Node) Get(key Key, value *Value) error {
	if between(hashString(elt.Address), hashString(string(key)), hashString(elt.Successors[0]), true) {
		*value = elt.Bucket[key]
	} else {
		call(elt.find(string(key)), "Get", key, value)
	}
	return nil
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
		// Fingers:     [161]string,
		Next: 0,
	}

	go func() {
		for {
			if active {
				node.stabilize()
				//node.checkPredecessor()
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
		case "put":
			if active {
				if len(commands) == 3 {
					keyvalue := KeyValue{Key(commands[1]), Value(commands[2])}

					err := call(node.Address, "Put", &keyvalue, &struct{}{})
					if err == nil {
						log.Printf("\tSUCCESS: '%s':'%s' was inserted into node", keyvalue.Key, keyvalue.Value)
					}
					//}
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
					err := call(node.Address, "Delete", commands[1], &struct{}{})
					if err == nil {
						log.Printf("\tSUCCESS: '%s' value was removed from node", commands[1])
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
						log.Printf("\n\tAddress:\t%s\n\tPredecessor:\t%s\n\tSuccessors:\t%v\n\tBucket:\t\t%v",
							values.Address, values.Predecessor, values.Successors, values.Bucket)
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
