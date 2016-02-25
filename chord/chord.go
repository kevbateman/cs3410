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

// Node holds a key-value pair.
type Node struct {
	Address     string
	Predecessor string
	Successors  [3]string
	Bucket      map[Key]Value
	mutex       sync.Mutex
}

func call(address string, method string, request interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	defer client.Close()
	if err != nil {
		log.Printf("\tError connecting to server at %s: %v", address, err)
	}
	client.Call("Node."+method, request, reply)
	return err
}

// Lookup iterates 32 times looking for a key
func (elt *Node) Lookup(key string, host *string) error {
	address := elt.Address
	for i := 0; i < 32; i++ {
		var successor string
		call(address, "FindSuccessor", elt.Successors[0], &successor)
		var keyExists bool
		call(successor, "Exists", key, &keyExists)
		if keyExists {
			log.Printf("Found key '%s' at '%s'", key, successor)
			*host = successor
			return nil
		}
		address = successor
	}
	return nil
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

// FindSuccessor asks Node elt to find the successor of address
func (elt *Node) FindSuccessor(address string, successor *string) error {
	if between(hashString(elt.Address),
		hashString(address),
		hashString(elt.Successors[0]),
		true) {
		*successor = elt.Successors[0]
	} else {
		call(elt.Successors[0], "FindSuccessor", address, successor)
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
		log.Printf("\tSetting node '%s' predecessor to '%s'", elt.Address, address)
		elt.Predecessor = address
	}
	return nil
}

// Stabilize is called every second
func (elt *Node) Stabilize(empty1 *struct{}, empty2 *struct{}) error {
	var x string
	call(elt.Successors[0], "GetPredecessor", &struct{}{}, &x)
	if between(hashString(elt.Address),
		hashString(x),
		hashString(elt.Successors[0]),
		false) && x != "" {
		log.Printf("\tSetting node.successor to '%s'", x)
		elt.Successors[0] = x
	}
	call(elt.Successors[0], "Notify", elt.Address, &struct{}{})
	return nil
}

// GetPredecessor simple returns the predecessor of node
func (elt *Node) GetPredecessor(empty1 *struct{}, predecessor *string) error {
	*predecessor = elt.Predecessor
	return nil
}

// Join an existing ring
func (elt *Node) Join(address string, empty *struct{}) error {
	elt.Predecessor = ""
	var successor string
	call(address, "FindSuccessor", elt.Address, &successor)
	log.Printf("\tSetting node.successor to '%s'", successor)
	elt.Successors[0] = successor
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
	if _, ok := elt.Bucket[key]; ok {
		delete(elt.Bucket, key)
	} else {

	}
	return nil
}

// Put inserts the given key and value into the currently active ring.
func (elt *Node) Put(pair *struct {
	Key   Key
	Value Value
}, empty *struct{}) error {
	elt.Bucket[pair.Key] = pair.Value
	var x string
	call(elt.Address, "FindSuccessor", elt.Successors[0], &x)
	//log.Printf("\tSUCCESS: key:value [%s:%s] was added to node", pair.Key, pair.Value)
	return nil
}

// Get finds the given key-value in the currently active ring.
func (elt *Node) Get(key Key, value *Value) error {
	if val, ok := elt.Bucket[key]; ok {
		*value = val
	}
	return nil
}

// Ping is used to ping between server and node
func (elt *Node) Ping(address string, empty *struct{}) error {
	//log.Printf("\tPing address = %s", address)
	if address == "SUCCESS" {
		log.Printf("\t '%s'", elt.Address)
	} else {
		err := call(address, "Ping", "SUCCESS", &struct{}{})
		if err != nil {
			log.Printf("\tFAILED: could not PING address '%s'", address)
		} else {
			log.Printf("\tSUCCESS: PINGED address '%s'", address)
		}
	}
	return nil
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
	address := getLocalAddress() + port

	go func() {
		for {
			if active {
				call(address, "Stabilize", &struct{}{}, &struct{}{})
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
				address = getLocalAddress() + port
			}
			log.Printf("\tPort is currently set to '%s'", port)
		case "create":
			if !active {
				node := Node{
					Address:     address,
					Predecessor: "",
					Successors:  [3]string{address},
					Bucket:      make(map[Key]Value),
				}
				create(&node, port)
				log.Printf("\tCreated ring at '%s'", address)
				active = true
			} else {
				log.Printf("\tFAILED: A ring has already been created or joined")
			}
		case "ping":
			if active {
				if len(commands) == 2 {
					call(address, "Ping", commands[1], &struct{}{})
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
					err := call(address, "Get", commands[1], &value)
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
					pair := struct {
						Key   Key
						Value Value
					}{Key(commands[1]), Value(commands[2])}
					err := call(address, "Put", &pair, &struct{}{})
					if err == nil {
						log.Printf("\tSUCCESS: '%s':'%s' was inserted into node", pair.Key, pair.Value)
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
					err := call(address, "Delete", commands[1], &struct{}{})
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
					err := call(address, "Dump", &struct{}{}, &values)
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
					node := Node{
						Address:     address,
						Predecessor: "",
						Successors:  [3]string{address},
						Bucket:      make(map[Key]Value),
					}
					create(&node, port)
					active = true
					err := call(address, "Join", commands[1], &struct{}{})
					if err == nil {
						//log.Printf("\tSUCCESS: Set '%s' as successor", commands[1])
						log.Printf("\tSUCCESS: Joined ring at '%s'", commands[1])
					} else {
						log.Fatalf("\tFAILED: Could not join ring at '%s'", commands[1])
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
					err := call(address, "Get", commands[1], &value)
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
