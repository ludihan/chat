package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

type channel struct {
	name           string
	isVoiceChannel bool
}

type options struct {
	port        string
	serverName  string
	serverImage *string
	channels    []channel
	file        *string
}

func parseChannels(channels string) []channel {
	parsedChannels := []channel{}
	for v := range strings.SplitSeq(channels, ",") {
		parsed := channel{}
		channel := strings.Split(v, ":")
		parsed.name = channel[0]
		channelType := channel[1]
		if channelType == "t" {
			parsed.isVoiceChannel = false
		} else {
			parsed.isVoiceChannel = true
		}
		parsedChannels = append(parsedChannels, parsed)
	}
	return parsedChannels
}

func setupOptions() options {
	port := flag.String("port", ":8080", "port used to host the server")
	serverName := flag.String("name", "server", "name used for the server")
	serverImage := flag.String("image", "", "image used for the server")
	channels := flag.String("channels", "general:t,voice:v", "channels available on the server")
	file := flag.String("file", "", "config file used to configure the server")
	help := flag.Bool("help", false, "prints this help")

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	return options{
		port:        *port,
		serverName:  *serverName,
		serverImage: serverImage,
		channels:    parseChannels(*channels),
		file:        file,
	}
}

func handleConnection(conn net.Conn) {
	var name string = ""
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading data:", err)
			break
		}
		if name == "" && message == "" {
			conn.Write([]byte("tell me your name"))
		}
		if name == "" && message != "" {
			name = message
		}
		fmt.Printf("Received message: %#v", message)
		conn.Write([]byte("ack:" + message))
	}
}

func main() {
	opts := setupOptions()
	listener, err := net.Listen("tcp", opts.port)
	if err != nil {
		fmt.Println("Failed to listen on port:", err)
		return
	}
	defer listener.Close()
	fmt.Fprintf(os.Stderr, "Server is listening on port %v...\n", opts.port)

	//userRegisterChan := make(chan string)

	go func() {
		//users := map[string]struct{}{}

		//newUser := <-userRegisterChan

	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}
