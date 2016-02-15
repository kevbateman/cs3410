package main

import (
	"bufio"
	"crypto/sha1"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

// Key is a string that is just used for distinction between other string types
type Key string

// Value is a string that is just used for distinction between other string types
type Value string

// Node holds a key-value pair.
type Node struct {
	Key         Key
	Value       Value
	Address     string
	Predecessor string
	Successors  []string
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

// Create initializes ring
func (elt *Node) Create(empty1 *struct{}, empty2 *struct{}) error {
	elt.mutex.Lock()
	defer elt.mutex.Unlock()
	var port string
	flag.StringVar(&port, "port", "3410", "port to listen on")
	flag.Parse()
	fmt.Printf("The port is %s\n", port)

	rpc.Register(elt)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	return nil
}

// Ping is used to ping between server and node
func (elt *Node) Ping(empty1 *struct{}, empty2 *struct{}) error {
	elt.mutex.Lock()
	defer elt.mutex.Unlock()
	log.Print("Ping!")
	return nil
}

// Register is required method of rpc
func (elt *Node) Register(address string, client *rpc.Client) error {
	elt.mutex.Lock()
	defer elt.mutex.Unlock()
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatalf("Error connecting to server at %s: %v", address, err)
	}
	if err = client.Call("Node.Ping", &struct{}{}, &struct{}{}); err != nil {
		log.Fatalf("Error calling node.CheckMessages: %v", err)
	}
	return nil
}
func main() {
	if len(os.Args) != 1 {
		log.Fatalf("Usage: %s <serveraddress>", os.Args[0])
	}
	var (
		client  rpc.Client
		err     error
		active  bool
		address string
	)
	active = false

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter command:")
	for scanner.Scan() {
		commands := strings.Split(scanner.Text(), " ")
		switch commands[0] {
		case "quit":
			if &client != nil {
				if err = client.Call("server.Shutdown", &struct{}{}, &struct{}{}); err != nil {
					log.Fatalf("Error calling server.Shutdown: %v", err)
				}
			}
			os.Exit(0)
		case "help":
			fmt.Println("\nCOMMANDS:")
			fmt.Println("    port <n>:\n\tset the port that this node should list on. Default is '3410'")
			fmt.Println("    quit:\n\t quit and ends the program")
			fmt.Println("    help:\n\t displays a list of recognized commands")
		case "port":
			if !active {
				if len(commands) == 1 {
					fmt.Println("defaulting address to 'localhost:3410'")
					fmt.Println()
					address = ":3410"
				} else {
					address = commands[1]
				}

				if strings.HasPrefix(address, ":") {
					address = "localhost" + address
				}
			} else {
				log.Fatalf("Cannot set port; a ring has already been created or joined. ")
			}
		case "create":
			if err = client.Call("Node.Create", &struct{}{}, &struct{}{}); err != nil {
				log.Fatalf("Error calling Node.Create: %v", err)
			}
			active = true
		case "ping":
			if err = client.Call("Node.Ping", &struct{}{}, &struct{}{}); err != nil {
				log.Fatalf("Error calling Node.Ping: %v", err)
			}
		default:
			fmt.Println("Unrecognized Command", scanner.Text())
		}
	}
}
