package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
	"github.com/linkedin/goavro"

	"badunicorngames.com/analytics/event"
)

var (
	brokers    = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	topic      = flag.String("topic", "Events", "The Kafka topic to consume")
	outdir     = flag.String("outdir", "./", "The directory where completed files are stored")
	fileprefix = flag.String("prefix", "analytics_", "The prefix to apply to all files written by this program")
	dump       = flag.Bool("dump", false, "If true, dump all incoming events to stdout")
)

var (
	codec goavro.Codec
)

var writerMap = make(map[string]*goavro.Writer)
var fileMap = make(map[string]*os.File)

func main() {

	var err error
	codec, err = goavro.NewCodec(event.AvroSchema)
	if err != nil {
		panic(err)
	}

	// Configuration options

	flag.Parse()

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))
	log.Println("Topic: ", *topic)

	// Test write capability

	testWritePermissions(*outdir)

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

	partitionConsumer, err := consumer.ConsumePartition(*topic, 0, sarama.OffsetOldest)
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

	keyCh := make(chan int, 1)
	go watchKeyboard(keyCh)

	// Consumer loop

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Message received, offset %d", msg.Offset)
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

		case <-keyCh:
			break ConsumerLoop
		}
	}

	log.Println("Exited ConsumerLoop")

	log.Println("Closing files")

	for _, wv := range writerMap {
		wv.Close()
	}
	for _, fv := range fileMap {
		fv.Close()
	}

	log.Println("Done")
}

func handleMessage(msg *event.Event) {

	if *dump {
		spew.Dump(msg)
	}

	writeMessage(msg)
}

func testWritePermissions(testPath string) {
	testDir := path.Join(testPath, "test")
	err := os.MkdirAll(testDir, 0777)
	if err != nil {
		panic("Failed to create test directory " + testDir)
	}

	testFile := filepath.Join(testDir, "testFile")
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		panic("Failed to create test file " + testFile)
	}
	file.Close()

	err = os.Remove(testFile)
	if err != nil {
		panic("Failed to delete test file " + testFile)
	}

	err = os.Remove(testDir)
	if err != nil {
		panic("Failed to delete test directory " + testDir)
	}
}

func writeMessage(msg *event.Event) {

	defaultObject := ""
	defaultData := make(map[string]interface{})

	// Message received
	if msg != nil {
		if msg.Object == nil {
			msg.Object = &defaultObject
		}

		// data field must be converted to map[string]interface{} for goavro
		data := &defaultData
		if msg.Data != nil {
			msgData := make(map[string]interface{})
			for k, v := range *msg.Data {
				msgData[k] = v
			}
			data = &msgData
		}

		record, err := goavro.NewRecord(goavro.RecordSchema(event.AvroSchema))
		if err != nil {
			log.Print(err)
		} else {

			if msg.Type != nil && msg.Producer != nil && msg.Subject != nil && msg.Verb != nil && msg.ClientIP != nil {
				record.Set("type", *msg.Type)
				record.Set("time", msg.Timestamp.UTC().String())
				record.Set("clientip", *msg.ClientIP)
				record.Set("producer", *msg.Producer)
				record.Set("subject", *msg.Subject)
				record.Set("verb", *msg.Verb)
				record.Set("object", *msg.Object)
				record.Set("data", *data)
				writer := getWriter(msg.Timestamp.UTC())
				writer.Write(record)
			}
		}
	}
}

func getWriter(msgTime time.Time) *goavro.Writer {

	// Build the full file path (which is also the map key) and make the folder if it doesn't exist yet
	outPath := path.Join(*outdir, fmt.Sprintf("%d", msgTime.Year()), fmt.Sprintf("%02d", msgTime.Month()), fmt.Sprintf("%02d", msgTime.Day()))
	os.MkdirAll(outPath, 0777)
	filename := fmt.Sprintf("%s%02d", *fileprefix, msgTime.Hour())
	fullFilepath := path.Join(outPath, filename)

	// Check the writers map first
	writer, ok := writerMap[fullFilepath]
	if ok && writer != nil {
		return writer
	}

	// No writer yet. The file should not be open yet either, but make sure before opening it.
	file, ok := fileMap[fullFilepath]
	var err error
	if !ok || file == nil {
		file, err = os.OpenFile(fullFilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			panic("Failed to open output file " + fullFilepath + ": " + err.Error())
		}
		log.Println("Opened output file " + fullFilepath)
	}

	// Set up the avro writer
	writer, err = codec.NewWriter(
		goavro.Compression(goavro.CompressionDeflate),
		goavro.BlockSize(10),
		goavro.BlockTick(time.Second*10),
		goavro.ToWriter(file))

	if err != nil {
		panic("Failed to create avro writer: " + err.Error())
	}

	// Update the maps
	fileMap[fullFilepath] = file
	writerMap[fullFilepath] = writer

	return writer
}

func watchKeyboard(ch chan int) {
	for {
		var s string
		_, err := fmt.Scan(&s)
		if err == nil && s == "c" {
			ch <- 1
			break
		}
	}
}
