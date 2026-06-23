package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
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
	for {
		select {
		case message := <-userBroadcastCh:
			fmt.Fprintf(conn, "%v:%v", message.user, message.body)
		default:
			reader := bufio.NewReader(conn)
			messageBody, err := reader.ReadString('\n')
			messageBody = strings.TrimSpace(messageBody)
			if err != nil {
				userUnregisterCh <- name
				fmt.Println("Error reading data:", err)
				break
			}

			if messageBody != "" && name != "" {
				serverBroadcastCh <- message{
					user: name,
					body: messageBody,
					time: time.Now(),
				}
			}

			if messageBody == "" && name == "" {
				fmt.Println(1)
				conn.Write([]byte("tell me your name:\n"))
			}
			if messageBody == "server" && name == "" {
				fmt.Println(2)
				conn.Write([]byte("you can't be the server, sorry\n"))
			}
			if messageBody != "" && name == "" {
				name = messageBody
				userRegisterCh <- connection{
					userName:    name,
					broadcastCh: userBroadcastCh,
				}
			}
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
				fmt.Println(2)
				delete(users, user)
				log.Println("UNregistered user:", user)
			case user := <-userRegisterCh:
				fmt.Println(1)
				users[user.userName] = struct{}{}
				userConnections = append(userConnections, user)
				user.broadcastCh <- message{
					user: "server",
					body: "welcome",
				}
				log.Println("registered user:", user.userName)
			case message := <-broadcastCh:
				fmt.Println(3)
				log.Printf("received message with body %v to user %v", message.body, message.user)
				for _, v := range userConnections {
					if v.userName != message.user {
						go func() {
							v.broadcastCh <- message
							log.Printf("send message with body %v to user %v", message.body, message.user)
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
		fmt.Println("Failed to listen on port:", err)
		return
	}
	defer listener.Close()

	fmt.Fprintf(os.Stderr, "Server is listening on port %v...\n", opts.port)

	go superv.supervisorProcess()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn, superv.userRegisterCh, superv.userUnregisterCh, superv.broadcastCh)
	}
}
