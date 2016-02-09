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
	var client_user string
	client_user = "kevin"
	if err = client.Call("ChatRoom.Register", &client_user, &reply); err != nil {
		log.Fatalf("Error calling ChatRoom.Say: %v", err)
	}
	log.Printf("Register returned %d", reply)
}
