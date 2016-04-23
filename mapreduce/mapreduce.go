package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func printHelp() {
	fmt.Println("\thelp:\n\t\tDisplays a list of recognized commands")
	fmt.Println("\tquit:\n\t\tEnds the program")
	fmt.Println("\tport <n>:\n\t\tSet listening port to <n>. Default is ':3410'")
	fmt.Println("\tcreate:\n\t\tCreate a new ring on the current port")
	fmt.Println("\tget <key>:\n\t\tGet the value from the current node corresponding to the given <key>")
	fmt.Println("\tput <key> <value>:\n\t\tGet the value from the current node corresponding to the given key")
}

func main() {
	if len(os.Args) != 1 {
		log.Fatalf("\t'mapreduce' takes no arguemnts")
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		commands := strings.Split(scanner.Text(), " ")
		switch commands[0] {
		case "help":
			printHelp()
		case "quit", "clear", "exit":
			log.Printf("\tShutting down\n\n")
			os.Exit(0)
		default:
			log.Printf("\tUnrecognized Command '%s'", scanner.Text())
		}
	}
}
