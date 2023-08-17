package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"
)

func getEnv(key string, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}

type LoadCfg struct {
	topic           string
	duration        int
	goroutines      int
	interrupted     int32
	statsAggregator chan *PubStats
}

type PubStats struct {
	NumMsg  int
	NumErrs int
}

func (cfg *LoadCfg) Stop() {
	atomic.StoreInt32(&cfg.interrupted, 1)
}

func (cfg *LoadCfg) RunSingleLoadSession(gid string, brokerConfig config.ServicePropertyMap) {

	messagingService, err := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(brokerConfig).Build()

	if err != nil {
		panic(err)
	}

	if err := messagingService.Connect(); err != nil {
		panic(err)
	}

	fmt.Println("Connected to the broker? ", messagingService.IsConnected(), gid)

	directPublisher, builderErr := messagingService.CreateDirectMessagePublisherBuilder().Build()
	if builderErr != nil {
		panic(builderErr)
	}

	startErr := directPublisher.Start()
	if startErr != nil {
		panic(startErr)
	}

	fmt.Println("Direct Publisher running? ", directPublisher.IsRunning(), gid)

	stats := &PubStats{}
	start := time.Now()

	msgSeqNum := 0

	//  Prepare outbound message payload and body
	messageBody := gid
	messageBuilder := messagingService.MessageBuilder().
		WithProperty("application", "sol-pub-bench").
		WithProperty("language", "go")

	for directPublisher.IsReady() && time.Since(start).Seconds() <= float64(cfg.duration) && atomic.LoadInt32(&cfg.interrupted) == 0 {
		msgSeqNum++
		message, err := messageBuilder.BuildWithStringPayload(messageBody + " --> " + strconv.Itoa(msgSeqNum))
		if err != nil {
			stats.NumErrs++
		}

		topic := resource.TopicOf(cfg.topic)

		publishErr := directPublisher.Publish(message, topic)
		if publishErr != nil {
			stats.NumErrs++
		} else {
			stats.NumMsg++
		}

	}

	directPublisher.Terminate(1 * time.Second)
	fmt.Println("Direct Publisher Terminated? ", directPublisher.IsTerminated())

	messagingService.Disconnect()
	fmt.Println("Messaging Service Disconnected? ", !messagingService.IsConnected())
	cfg.statsAggregator <- stats
}

const APP_VERSION = "0.1"

var goroutines int = 2
var duration int = 10 //seconds
var solaceHost string = "tcp://localhost:55551"
var solaceVPN string = "default"
var solaceUsername string = "admin"
var solacePwd string = "admin"
var topic string = "sol-pub-bench"

var statsAggregator chan *PubStats

func init() {
	flag.IntVar(&goroutines, "c", 10, "Number of goroutines to use (concurrent connections)")
	flag.IntVar(&duration, "d", 10, "Duration of test in seconds")
	flag.StringVar(&solaceHost, "host", "tcp://localhost:55555", "Solace host")
	flag.StringVar(&solaceVPN, "vpn", "default", "Solace vpn")
	flag.StringVar(&solaceUsername, "username", "admin", "Solace username")
	flag.StringVar(&solacePwd, "pwd", "admin", "Solace password")
	flag.StringVar(&solacePwd, "topic", "sol-pub-bench", "Solace topic to publish")
}

func main() {
	// logging.SetLogLevel(logging.LogLevelInfo)
	fmt.Printf("Connecting to %s %s \n", solaceHost, solaceVPN)
	flag.Parse()
	brokerConfig := config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                solaceHost,
		config.ServicePropertyVPNName:                    solaceVPN,
		config.AuthenticationPropertySchemeBasicPassword: solacePwd,
		config.AuthenticationPropertySchemeBasicUserName: solaceUsername,
	}

	fmt.Printf("Running %vs test @ %v\n  %v goroutine(s) running concurrently\n", duration, topic, goroutines)

	// Handle OS interrupts
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	statsAggregator = make(chan *PubStats, goroutines)

	loader := &LoadCfg{topic, duration, goroutines, 0, statsAggregator}

	for i := 0; i < goroutines; i++ {
		go loader.RunSingleLoadSession(strconv.Itoa(i), brokerConfig)
	}
	responders := 0

	for responders < goroutines {
		select {
		case <-c:
			loader.Stop()
			fmt.Printf("stopping...\n")
		case stats := <-statsAggregator:
			fmt.Printf("Publisher id:%v\n", responders)
			fmt.Printf("Number of Errors:\t%v\n", stats.NumErrs)
			fmt.Printf("Msg seq\t%v\n", stats.NumMsg)
			fmt.Println()

			responders++
		}
	}

}
