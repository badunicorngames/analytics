package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"gopkg.in/alexcesaro/statsd.v2"

	"badunicorngames.com/analytics/event"
)

var (
	brokers       = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	topic         = flag.String("topic", "Events", "The Kafka topic to consume")
	strict        = flag.Bool("strict", true, "If true, only process messages within one hour of the current time")
	statsd_server = flag.String("statsd", "localhost:8125", "The address:port of the statsd server")
	oldest        = flag.Bool("offset_oldest", false, "If true, emit stats from the oldest event in kafka. Otherwise, only handle new events.")
)

var statsd_client *statsd.Client

func main() {

	// Configuration options

	flag.Parse()

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))
	log.Println("Topic: ", *topic)
	log.Println("statsd: ", *statsd_server)

	// Set up statsd

	var err error
	statsd_client, err = statsd.New(statsd.Address(*statsd_server), statsd.FlushPeriod(1*time.Second), statsd.Mute(false))
	if err != nil {
		panic("statsd setup failed: " + err.Error())
	}
	defer statsd_client.Close()

	// Set up the Kafka consumer

	consumer, err := sarama.NewConsumer(brokerList, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		err := consumer.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(*topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	defer func() {
		err := partitionConsumer.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT for shutdown

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Consumer loop

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			//log.Printf("Message received, offset %d", msg.Offset)
			key := string(msg.Key)
			if key == "event" {
				var event event.Event
				err := json.Unmarshal(msg.Value, &event)
				if err != nil {
					log.Println("JSON unmarshal failed: ", err)
				} else {
					handleMessage(&event)
				}
			}

		case <-signals:
			break ConsumerLoop
		}
	}

	log.Println("Exited ConsumerLoop")
}

func handleMessage(msg *event.Event) {

	// Validate time

	var currentHour = time.Now().UTC().Hour()
	var msgHour = msg.Timestamp.UTC().Hour()

	if *strict {
		var deltaHour = currentHour - msgHour
		if deltaHour < -1 || deltaHour > 1 {
			log.Printf("Rejected message with deltaHour %d", deltaHour)
			return
		}
	}

	// Validate fields

	if msg.Type == nil || msg.ClientIP == nil || msg.Producer == nil || msg.Subject == nil || msg.Verb == nil {
		log.Printf("Rejected message with one or more missing fields")
		return
	}

	//log.Printf("Received %+v\n", msg)

	// fix my stupidity
	*msg.Producer = strings.ToLower(*msg.Producer)

	eventType := *msg.Subject + "_" + *msg.Verb
	if msg.Object != nil {
		eventType += "_" + *msg.Object
	}

	// Delegate to specific event handlers if possible
	if strings.HasPrefix(eventType, "game_loaded") {
		handleGameLoaded(msg)
	} else if strings.HasPrefix(eventType, "game_finished") {
		handleGameFinished(msg)
	} else {
		// Otherwise, just emit a standard counter metric
		baseEvent := eventType + "." + *msg.Producer
		statsd_client.Increment(baseEvent)
	}
}

func handleGameLoaded(msg *event.Event) {

	// Base event plus tags
	baseEvent := "game_loaded." + *msg.Producer
	if msg.Data != nil {
		platform, ok := (*msg.Data)["platform"]
		if ok && len(platform) > 0 {
			baseEvent += ",platform=" + platform
		}
		browser, ok := (*msg.Data)["browser"]
		if ok && len(browser) > 0 {
			baseEvent += ",browser=" + browser
		}
		domain, ok := (*msg.Data)["domain"]
		if ok && len(domain) > 0 {
			baseEvent += ",domain=" + domain
		}
	}
	statsd_client.Increment(baseEvent)

	// Unique users
	clientIP := *msg.ClientIP
	statsd_client.Unique("unique_users."+*msg.Producer, clientIP)
}

func handleGameFinished(msg *event.Event) {
	// Save victory state now because it will be used as a tag for multiple metrics
	victoryTag := ""
	if msg.Data != nil {
		victory, ok := (*msg.Data)["victory"]
		if ok {
			victoryBool, err := strconv.ParseBool(victory)
			if err == nil {
				victoryTag = ",victory=" + strconv.FormatBool(victoryBool)
			} else {
				log.Printf("Failed to parse victory value '%s' as bool", victory)
			}
		}
	}

	// Base event plus tags
	baseEvent := "game_finished." + *msg.Producer + victoryTag
	statsd_client.Increment(baseEvent)

	if msg.Data != nil {
		// Duration metric
		durationString, ok := (*msg.Data)["duration_millis"]
		duration, err := strconv.Atoi(durationString)
		if ok && err == nil {
			durationBucket := "game_finished_duration." + *msg.Producer + victoryTag
			statsd_client.Timing(durationBucket, duration)
		}

		// Score metric
		scoreString, ok := (*msg.Data)["score"]
		score, err := strconv.Atoi(scoreString)
		if ok && err == nil {
			scoreBucket := "game_finished_score." + *msg.Producer + victoryTag
			statsd_client.Timing(scoreBucket, score)
		}

		// Accuracy metric
		accuracyString, ok := (*msg.Data)["accuracy"]
		accuracy, err := strconv.Atoi(accuracyString)
		if ok && err == nil {
			accuracyBucket := "game_finished_accuracy." + *msg.Producer + victoryTag
			statsd_client.Timing(accuracyBucket, accuracy)
		}
	}
}
