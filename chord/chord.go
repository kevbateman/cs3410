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
	Successors  []string
	Bucket      map[Key]Value
	mutex       sync.Mutex
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

// Dump lists the key/value pairs stored locally.
func (elt *Node) Dump(empty1 *struct{}, info *Node) error {
	info.Address = elt.Address
	info.Predecessor = elt.Predecessor
	info.Successors = elt.Successors
	info.Bucket = elt.Bucket
	return nil
}

// Delete has the peer remove the given key-value from the currently active ring.
func (elt *Node) Delete(key *Key, empty *struct{}) error {
	if _, ok := elt.Bucket[*key]; ok {
		delete(elt.Bucket, *key)
		// 	log.Printf("\tSUCCESS (Deletion): key '%s' and its value were removed from node", *key)
		// } else {
		// 	log.Printf("\tFAILED (Deletion attempt: key '%s' does not exist in node", *key)
	}
	return nil
}

// Put inserts the given key and value into the currently active ring.
func (elt *Node) Put(pair *struct {
	Key   Key
	Value Value
}, empty *struct{}) error {
	elt.Bucket[pair.Key] = pair.Value
	//log.Printf("\tSUCCESS: key:value [%s:%s] was added to node", pair.Key, pair.Value)
	return nil
}

// Get finds the given key-value in the currently active ring.
func (elt *Node) Get(key *Key, value *Value) error {
	if val, ok := elt.Bucket[*key]; ok {
		*value = val
		// } else {
		// 	log.Printf("\tFAILED: key '%s' does not exist in node", *key)
	}
	return nil
}

// Ping is used to ping between server and node
func (elt *Node) Ping(address string, empty *struct{}) error {
	_, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Printf("\tFailed to PING(%s): %v", address, err)
	} else {
		log.Printf("\tSUCCESS: PING(%s)", address)
	}
	return nil
}

func main() {
	fmt.Println()
	if len(os.Args) != 1 {
		log.Fatalf("\t'chord' takes no arguemnts")
	}

	var (
		client *rpc.Client
		err    error
	)

	port := ":3410"
	active := false
	address := getLocalAddress() + port
	node := Node{
		Address:     address,
		Predecessor: "",
		Successors:  []string{address},
		Bucket:      make(map[Key]Value),
	}
	rpc.Register(&node)
	rpc.HandleHTTP()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Default port is ':3410'\nEnter command:")
	for scanner.Scan() {
		commands := strings.Split(scanner.Text(), " ")
		switch commands[0] {
		case "help":
			fmt.Println("\thelp:\n\t\tDisplays a list of recognized commands")
			fmt.Println("\tquit:\n\t\tEnds the program")
			fmt.Println("\tport <n>:\n\t\tSet listening port to <n>. Default is ':3410'")
			fmt.Println("\tcreate:\n\t\tCreate a new ring on the current port")
			fmt.Println("\tget <key>:\n\t\tGet the value from the current node corresponding to the given <key>")
			fmt.Println("\tput <key> <value>:\n\t\tGet the value from the current node corresponding to the given key")
		case "quit":
			log.Printf("\tShutting down\n\n")
			os.Exit(0)
		case "port":
			if active {
				log.Printf("\tFAILED: Cannot change port; a ring has already been created or joined on this port")
			} else if len(commands) == 2 {
				port = commands[1]
				address = getLocalAddress() + port
			}
			log.Printf("\tPort is currently set to '%s'", port)
		case "create":
			if !active {
				go func() {
					log.Fatal(http.ListenAndServe(port, nil), "")
				}()
				time.Sleep(time.Second)
				log.Printf("\tNew ring created on port '%s'", port)

				client, err = rpc.DialHTTP("tcp", address)
				if err != nil {
					log.Printf("\tError connecting to ring at %s: %v", address, err)
				} else {
					log.Printf("\tSUCCESS: Connected node to ring at '%s'", address)
					active = true
				}
			} else {
				log.Printf("\tFAILED: A ring has already been created or joined on this port")
			}
		case "ping":
			if active {
				if len(commands) == 2 {
					if err = client.Call("Node.Ping", commands[1], &struct{}{}); err != nil {
						log.Printf("\tError calling Node.Ping: %v", err)
					}
				} else if len(commands) == 1 {
					log.Printf("\tFAILED: Missing <port> parameter; use 'ping <port>' command: ")
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'ping <port>' command")
				}
			} else {
				log.Printf("\tFAILED: Node must be active in order to 'ping <port>'")
			}
		case "get":
			if active {
				if len(commands) == 2 {
					var value string
					if err = client.Call("Node.Get", &commands[1], &value); err != nil {
						log.Printf("\tError calling Node.Get: %v", err)
					}
					if len(value) > 0 {
						log.Printf("\tSUCCESS: Retrieved value '%s'", value)
					} else {
						log.Printf("\tFAILED: Could not retrieve a value with key '%s'", commands[1])
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
					if err = client.Call("Node.Put", &pair, &struct{}{}); err != nil {
						log.Printf("\tError calling Node.Put: %v", err)
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
					if err = client.Call("Node.Delete", &commands[1], &struct{}{}); err != nil {
						log.Printf("\tError calling Node.Delete: %v", err)
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
					if err = client.Call("Node.Dump", &struct{}{}, &values); err != nil {
						log.Printf("\tError calling Node.Dump: %v", err)
					}
					log.Printf("\n\tAddress:\t%s\n\tPredecessor:\t%s\n\tSuccessors:\t%v\n\tBucket:\t\t%v",
						values.Address, values.Predecessor, values.Successors, values.Bucket)
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'dump' command")
				}
			} else {
				log.Printf("\tFAILED: Node must be active to 'dump' values")
			}
		case "join":
			if !active {
				if len(commands) == 2 {
					go func() {
						log.Fatal(http.ListenAndServe(port, nil), "")
					}()
					time.Sleep(time.Second)
					log.Printf("\tNew node created on port '%s'", port)

					client, err = rpc.DialHTTP("tcp", address)
					if err != nil {
						log.Printf("\tError connecting to server at %s: %v", commands[1], err)
					} else {
						log.Printf("\tSUCCESS: Connected node to server at '%s'", commands[1])
						active = true
					}
				} else if len(commands) == 1 {
					log.Printf("\tFAILED: Missing <address> parameter; use 'join <address>' command: ")
				} else {
					log.Printf("\tFAILED: Too many parameters given; use 'join <address>' command")
				}
			} else {
				log.Printf("\tFAILED: A ring has already been join or created on this port")
			}
		default:
			log.Printf("\tUnrecognized Command '%s'", scanner.Text())
		}
	}
}
