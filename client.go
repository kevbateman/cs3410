package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type Memo struct {
	Sender, Target, Message string
}

type Record struct {
	Sender, Message string
}

func main() {
	if len(os.Args) != 3 {
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

	username := os.Args[2]
	if err = client.Call("ChatRoom.Register", &username, &struct{}{}); err != nil {
		log.Fatalf("Error calling ChatRoom.Say: %v", err)
	}

	go func() {
		for {
			var messages []string
			if err = client.Call("ChatRoom.CheckMessages", &username, &messages); err != nil {
				log.Fatalf("Error calling ChatRoom.CheckMessages: %v", err)
			}
			for _, message := range messages {
				fmt.Println(message)
			}
			time.Sleep(time.Second)
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("Enter command:\n")
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		switch scanner.Text() {
		case "tell":
			scanner.Scan()
			target := scanner.Text()
			// set scanner to read line
			scanner.Split(bufio.ScanLines)
			scanner.Scan()
			message := scanner.Text()

			if err = client.Call("ChatRoom.Tell", &Memo{username, target, message}, &struct{}{}); err != nil {
				log.Fatalf("Error calling ChatRoom.Tell: %v", err)
			}
			// set scanner back to reading single words
			scanner.Split(bufio.ScanWords)
		case "say":
			// set scanner to read line
			scanner.Split(bufio.ScanLines)
			scanner.Scan()
			message := scanner.Text()

			if err = client.Call("ChatRoom.Say", &Record{username, message}, &struct{}{}); err != nil {
				log.Fatalf("Error calling ChatRoom.Say: %v", err)
			}
			// set scanner back to reading single words
			scanner.Split(bufio.ScanWords)
		case "shutdown":
			if err = client.Call("ChatRoom.Shutdown", &struct{}{}, &struct{}{}); err != nil {
				log.Fatalf("Error calling ChatRoom.Shutdown: %v", err)
			}
			os.Exit(0)
		case "quit":
			if err = client.Call("ChatRoom.Logout", username, &struct{}{}); err != nil {
				log.Fatalf("Error calling ChatRoom.Logout: %v", err)
			}
			os.Exit(0)
		case "list":
			var reply []string
			if err = client.Call("ChatRoom.List", &struct{}{}, &reply); err != nil {
				log.Fatalf("Error calling ChatRoom.List: %v", err)
			}
			fmt.Println(reply)
		case "help":
			fmt.Println("\nCOMMANDS:")
			fmt.Println("  tell <user> <message>:\n    Sends specific <user> a <message>.")
			fmt.Println("  say <message>:\n    Sends a <message> to everyone in the room.")
			fmt.Println("  list:\n    Lists all users currently in the room.")
			fmt.Println("  help:\n    Displays all of the commands to use.\n")
		default:
			fmt.Println("Unrecognized Command", scanner.Text())
		}
	}
}
