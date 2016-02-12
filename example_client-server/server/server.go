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

type Counter int

func (c *Counter) Increment(n Counter, value *Counter) error {
	log.Printf("Increment called: %d + %d = %d", *c, n, *c+n)
	*c += n
	*value = *c
	return nil
}

func (c *Counter) Get(junk Nothing, value *Counter) error {
	log.Printf("Get called: %d", *c)
	*value = *c
	return nil
}

func main() {
	var port string
	flag.StringVar(&port, "port", "3410", "port to listen on")
	flag.Parse()
	fmt.Printf("The port is %s\n", port)

	counter := new(Counter)
	rpc.Register(counter)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
