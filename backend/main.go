package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"
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

type connection struct {
	userName    string
	broadcastCh chan message
}

type message struct {
	user string
	body string
	time time.Time
}

type supervisor struct {
	userRegisterCh    chan connection
	userUnregisterCh  chan string
	users             map[string]connection
	supervisorProcess func()
	broadcastCh       chan message
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

func handleConnection(
	conn net.Conn,
	userRegisterCh chan connection,
	userUnregisterCh chan string,
	serverBroadcastCh chan message,
) {
	conn.Write([]byte("tell me your name:\n"))
	var name string = ""
	defer conn.Close()
	userBroadcastCh := make(chan message)

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)
	for {
		go func() {
			message := <-userBroadcastCh
			fmt.Fprintf(conn, "%v:%v", message.user, message.body)
		}()
		go func() {
			<-sigch
			conn.Close()
			os.Exit(0)
		}()
		reader := bufio.NewReader(conn)
		messageBody, err := reader.ReadString('\n')
		messageBody = strings.TrimSpace(messageBody)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("user \"%v\" disconnected\n", name)
			} else {
				log.Printf("error reading for user \"%v\" data: %v\n", name, err)
			}
			userUnregisterCh <- name
			return
		}

		if messageBody != "" && name != "" {
			serverBroadcastCh <- message{
				user: name,
				body: messageBody + "\n",
				time: time.Now(),
			}
		}

		if messageBody == "" && name == "" {
			conn.Write([]byte("tell me your name:\n"))
		}
		if messageBody == "server" && name == "" {
			conn.Write([]byte("you can't be the server, sorry\n"))
		}
		if messageBody != "" && name == "" {
			name = messageBody
			userRegisterCh <- connection{
				userName:    name,
				broadcastCh: userBroadcastCh,
			}
			go func() {
				message := <-userBroadcastCh
				fmt.Fprintf(conn, "%v:%v", message.user, message.body)
			}()
		}
	}
}

func createSupervisor() supervisor {
	userRegisterCh := make(chan connection)
	userUnregisterCh := make(chan string)
	broadcastCh := make(chan message)

	routine := func() {
		users := map[string]struct{}{"server": {}}
		userConnections := []connection{}

		for {
			select {
			case user := <-userUnregisterCh:
				delete(users, user)
				log.Printf("unregistered user: \"%v\"\n", user)
			case user := <-userRegisterCh:
				users[user.userName] = struct{}{}
				userConnections = append(userConnections, user)
				user.broadcastCh <- message{
					user: "server",
					body: "welcome " + user.userName + "\n",
					time: time.Now(),
				}
				log.Println("registered user:", user.userName)
			case message := <-broadcastCh:
				log.Printf("received message with body %v from user %v", strings.TrimSpace(message.body), message.user)
				for _, v := range userConnections {
					if v.userName != message.user {
						go func() {
							v.broadcastCh <- message
							log.Printf("send message with body %v to user %v", strings.TrimSpace(message.body), message.user)
						}()
					}
				}
			}
		}
	}

	return supervisor{
		userRegisterCh:    userRegisterCh,
		userUnregisterCh:  userUnregisterCh,
		users:             map[string]connection{},
		supervisorProcess: routine,
		broadcastCh:       broadcastCh,
	}
}

func main() {
	opts := setupOptions()
	superv := createSupervisor()

	listener, err := net.Listen("tcp", opts.port)
	if err != nil {
		log.Fatalln("Failed to listen on port:", err)
	}
	defer listener.Close()

	log.Printf("Server is listening on port %v...\n", opts.port)

	go superv.supervisorProcess()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn, superv.userRegisterCh, superv.userUnregisterCh, superv.broadcastCh)
	}
}
