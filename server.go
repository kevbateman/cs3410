package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
  "strings"
)

type Nothing struct{}

type ChatRoom struct {
  users map[string][]string
}

type Memo struct {
  sender,target,message string
}

func (room *ChatRoom) Register(user *string, junk *Nothing) error {
	log.Printf(user, "joined the room")
  for k,_ := range room.users {
    room.users[k] = append(room.users[k],strings.Join(user, " has joined the room"))
  }
	return nil
}

func (room *ChatRoom) List(junk *Nothing, online *[]string) error {
	log.Printf("listing online users")
  for k,_ := range room.users {
    online = append(online,k)
  }
	return nil
}

func (room *ChatRoom) CheckMessages(user *string, messages *[]string) error {
	log.Printf("checking",user, "messages")
  for _,message := range room.users[user] {
    messages = append(messages, message)
  }
	return nil
}

func (room *ChatRoom) Tell(memo *Memo, junk *Nothing) error {
  _, ok := room.users[memo.target]
  if ok {
    room.users[memo.target] = append(room.user[memo.target], memo.message);
  } else {
    room.users[memo.sender] = append(room.users[memo.sender], strings.Join(memo.target, " did not get your message '", memo.message,"'"))
  }
	return nil
}

func (room *ChatRoom) Say(sender *string, message *string) error {
	log.Printf(sender, "says", string)
  for k,_ := range room.users {
    room.users[k] = append(room.users[k],message)
  }
	return nil
}

func (room *ChatRoom) Logout(user *string) error {
	log.Printf(user, "logged out")
  for k,_ := range room.users {
    room.users[k] = append(room.users[k],strings.Join(user, " has logged out"))
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
