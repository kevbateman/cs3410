package main

import (
	"log"
	"net/rpc"
	"os"
	"strings"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <serveraddress>", os.Args[0])
	}
	address := os.Args[1]
	if strings.HasPrefix(address, ":") {
		address = "localhost" + address
	}

	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatalf("Error connecting to server at %s: %v", address, err)
	}

	var reply int
	arg := 5
	if err = client.Call("Counter.Increment", arg, &reply); err != nil {
		log.Fatalf("Error calling Counter.Increment: %v", err)
	}
	log.Printf("Increment returned %d", reply)

	reply = -1
	if err = client.Call("Counter.Get", struct{}{}, &reply); err != nil {
		log.Fatalf("Error calling Counter.Get: %v", err)
	}
	log.Printf("Get returned %d", reply)
}
