package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Nothing struct{}

type ChatRoom struct {
	users map[string][]string
}

type Memo struct {
	sender, target, message string
}
type Record struct {
	sender, message string
}

func (room *ChatRoom) Register(user *string, empty *Nothing) error {
	log.Printf(*user, "joined the room")
	for k, _ := range room.users {
		room.users[k] = append(room.users[k], *user + " has joined the room")
	}
	return nil
}

func (room *ChatRoom) List(empty *Nothing, online *[]string) error {
	log.Printf("listing online users")
	for k, _ := range room.users {
		*online = append(*online, k)
	}
	return nil
}

func (room *ChatRoom) CheckMessages(user *string, messages *[]string) error {
	log.Printf("checking", user, "messages")
	for _, message := range room.users[*user] {
		*messages = append(*messages, message)
	}
	return nil
}

func (room *ChatRoom) Tell(memo *Memo, empty *Nothing) error {
	log.Printf(memo.sender, "tells", memo.target, memo.message)
	_, ok := room.users[memo.target]
	if ok {
		room.users[memo.target] = append(room.users[memo.target], memo.sender+" tells you "+memo.message)
	} else {
		room.users[memo.sender] = append(room.users[memo.sender],memo.target+" did not get your message '"+memo.message+ "'")
	}
	return nil
}

func (room *ChatRoom) Say(record *Record, empty *Nothing) error {
	log.Printf(record.sender, "says", record.message)
	for k, _ := range room.users {
		room.users[k] = append(room.users[k], record.sender+" says "+record.message)
	}
	return nil
}

func (room *ChatRoom) Logout(user *string, empty *Nothing) error {
	log.Printf(*user, "logged out")
	delete(room.users, *user)
	for k, _ := range room.users {
		room.users[k] = append(room.users[k], *user+" has logged out")
	}
	return nil
}

func main() {
	var port string
	flag.StringVar(&port, "port", "3410", "port to listen on")
	flag.Parse()
	fmt.Printf("The port is %s\n", port)

	room := new(ChatRoom)
	rpc.Register(room)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
