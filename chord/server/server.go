package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Nothing struct{}

type ChatRoom struct {
	users map[string][]string
	shutdown chan bool
	mutex sync.Mutex
}
type Memo struct {
	Sender, Target, Message string
}
type Record struct {
	Sender, Message string
}

func (room *ChatRoom) Register(user *string, empty *Nothing) error {
	room.mutex.Lock()
	defer room.mutex.Unlock()
	fmt.Println(*user, "joined the room")
	room.users[*user] = make([]string,0)
	for k, _ := range room.users {
		room.users[k] = append(room.users[k], *user + " has joined the room")
	}
	return nil
}

func (room *ChatRoom) List(empty *Nothing, online *[]string) error {
	room.mutex.Lock()
	defer room.mutex.Unlock()
	//fmt.Println("listing online users")
	for k, _ := range room.users {
		*online = append(*online, "\n    ",k)
	}
	*online = append(*online,"\n")
	return nil
}

func (room *ChatRoom) CheckMessages(user *string, messages *[]string) error {
	room.mutex.Lock()
	defer room.mutex.Unlock()
	//fmt.Println("checking", *user, "messages")
	for _, message := range room.users[*user] {
		*messages = append(*messages, message)
	}
	room.users[*user] = room.users[*user][:0]
	return nil
}

func (room *ChatRoom) Tell(memo *Memo, empty *Nothing) error {
	room.mutex.Lock()
	defer room.mutex.Unlock()
	_, ok := room.users[memo.Target]
	if ok {
		//fmt.Println(memo.Sender, "tells", memo.Target, "'",memo.Message,"'")
		room.users[memo.Target] = append(room.users[memo.Target], memo.Sender+" tells you "+memo.Message)
	} else {
		//fmt.Println(memo.Sender, "tells", memo.Target, "'",memo.Message,"' but they didn't get the message")
		room.users[memo.Sender] = append(room.users[memo.Sender],memo.Target+" did not get your message '"+memo.Message+ "'")
	}
	return nil
}

func (room *ChatRoom) Say(record *Record, empty *Nothing) error {
	room.mutex.Lock()
	defer room.mutex.Unlock()
	//fmt.Println(record.Sender, "says", record.Message)
	for k, _ := range room.users {
		room.users[k] = append(room.users[k], record.Sender+" says "+record.Message)
	}
	return nil
}

func (room *ChatRoom) Logout(user *string, empty *Nothing) error {
	room.mutex.Lock()
	defer room.mutex.Unlock()
	//fmt.Println(*user, "logged out")
	delete(room.users, *user)
	for k, _ := range room.users {
		room.users[k] = append(room.users[k], *user+" has logged out")
	}
	return nil
}

func (room *ChatRoom) Shutdown(empty1 *Nothing, empty2 *Nothing) error {
	room.mutex.Lock()
	defer room.mutex.Unlock()
	//fmt.Println("Shutting down server")
	room.shutdown <- true
	return nil
}

func main() {
	var port string
	flag.StringVar(&port, "port", "3410", "port to listen on")
	flag.Parse()
	fmt.Printf("The port is %s\n", port)

	room := &ChatRoom{users:make(map[string][]string)}

	go func () {
		_,ok := <- room.shutdown
		if ok {
			os.Exit(0)
		}
	}()

	rpc.Register(room)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
