package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/cors"

	"badunicorngames.com/analytics/event"
)

// We need to implement sarama.Encoder for Event, so wrap it in a new type
type saramaEvent struct {
	event   *event.Event
	encoded []byte
	err     error
}

var (
	listen  = flag.String("listen", ":80", "The address to bind to")
	brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
)

func main() {

	// Configuration options

	flag.Parse()

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	// Set up the Kafka producer

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = false

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		panic(err)
	}

	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write event: ", err)
		}
	}()

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	// Start up the HTTP server

	mux := http.NewServeMux()
	mux.HandleFunc("/event", KafkaProducerHandler(producer))
	handler := cors.Default().Handler(mux)

	log.Printf("[INFO] starting server on %s", *listen)
	err = http.ListenAndServe(*listen, handler)
	if err != nil {
		panic(err)
	}
}

func KafkaProducerHandler(producer sarama.AsyncProducer) http.HandlerFunc {
	return func(writer http.ResponseWriter, req *http.Request) {
		//log.Printf("[INFO] %s %s%s", req.Method, req.URL.Host, req.URL.Path)

		// Validate: POST only
		if req.Method != http.MethodPost {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}

		// Decode incoming JSON to event.Event
		// Validate: Body must contain valid JSON
		decoder := json.NewDecoder(req.Body)
		var event event.Event
		err := decoder.Decode(&event)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}

		// Validate: Must contain required fields
		if event.Type == nil || event.Producer == nil || event.Subject == nil || event.Verb == nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}

		// TODO: Validate schema

		// Validation successful
		writer.WriteHeader(http.StatusOK)
		defer req.Body.Close()

		// Fill in server-provided fields
		now := time.Now().UTC()
		event.Timestamp = &now

		clientIP := req.Header.Get("X-Real-IP")
		event.ClientIP = &clientIP

		// Pipe to Kafka
		outEvent := &saramaEvent{
			event:   &event,
			encoded: nil,
			err:     nil,
		}

		msg := &sarama.ProducerMessage{
			Topic: "Events",
			Key:   sarama.StringEncoder("event"),
			Value: outEvent,
		}

		producer.Input() <- msg
	}
}

// Implement sarama.Encoder for saramaEvent

func (self *saramaEvent) lazyEncode() {
	if self.encoded == nil && self.err == nil {
		self.encoded, self.err = json.Marshal(self.event)
	}
}

func (self *saramaEvent) Encode() ([]byte, error) {
	self.lazyEncode()
	return self.encoded, self.err
}

func (self *saramaEvent) Length() int {
	self.lazyEncode()
	return len(self.encoded)
}
