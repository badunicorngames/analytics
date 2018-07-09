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
	tmpdir     = flag.String("tmpdir", "/tmp/", "The directory where files are stored until they are marked complete")
	outdir     = flag.String("outdir", "./", "The directory where completed files are stored")
	fileprefix = flag.String("prefix", "analytics_", "The prefix to apply to all files written by this program")
	dump       = flag.Bool("dump", false, "If true, dump all incoming events to stdout")
	strict     = flag.Bool("strict", true, "If true, only process messages within one hour of the current time")
)

var (
	codec goavro.Codec
)

var fileChan [24]chan *event.Event
var exitChan [24]chan int
var exitedChan [24]chan int

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

	testWritePermissions(*tmpdir)
	testWritePermissions(*outdir)

	// Set up file writers

	for h := 0; h < 24; h++ {
		fileChan[h] = make(chan *event.Event, 1000)
		exitChan[h] = make(chan int, 1)
		exitedChan[h] = make(chan int, 1)
		go fileWriter(h)
	}
	rotatorExitCh := make(chan int)
	go fileRotator(rotatorExitCh)

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
	for i := range fileChan {
		exitChan[i] <- 1
		close(fileChan[i])
	}
	rotatorExitCh <- 1
}

func handleMessage(msg *event.Event) {
	var currentHour = time.Now().UTC().Hour()
	var msgHour = msg.Timestamp.UTC().Hour()

	if *strict {
		var deltaHour = currentHour - msgHour
		if deltaHour < -1 || deltaHour > 1 {
			log.Printf("Rejected message with deltaHour %d", deltaHour)
			return
		}
	}

	//log.Printf("Received %+v\n", msg)
	if *dump {
		spew.Dump(msg)
	}

	fileChan[msgHour] <- msg
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

func fileWriter(hour int) {

	ch := fileChan[hour]
	exitCh := exitChan[hour]
	exitedCh := exitedChan[hour]

	defer func() {
		exitedCh <- 1
	}()

	// Open the output file

	filename := fmt.Sprintf("%s%02d", *fileprefix, hour)
	fullFilepath := filepath.Join(*tmpdir, filename)

	_, err := os.Stat(fullFilepath)
	if err == nil {
		// Delete it first if it already exists
		// We do not want to append because we do not want any old data
		os.Remove(fullFilepath)
	}

	file, err := os.OpenFile(fullFilepath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic("Failed to open output file " + fullFilepath)
	}
	log.Println("Opened output file " + fullFilepath)
	defer file.Close()

	// Set up the avro writer

	writer, err := codec.NewWriter(
		goavro.Compression(goavro.CompressionDeflate),
		goavro.BlockSize(10),
		goavro.BlockTick(time.Second*10),
		goavro.ToWriter(file))

	if err != nil {
		panic(err)
	}

	defer func() {
		err := writer.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	defaultObject := ""
	defaultData := make(map[string]interface{})

	// Loop forever, watching for messages until the channel closes

MessageLoop:
	for {
		select {

		case <-exitCh:
			break MessageLoop

		case msg, ok := <-ch:

			// Channel closed
			if !ok {
				break MessageLoop
			}

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
					record.Set("type", *msg.Type)
					record.Set("time", msg.Timestamp.UTC().String())
					record.Set("clientip", *msg.ClientIP)
					record.Set("producer", *msg.Producer)
					record.Set("subject", *msg.Subject)
					record.Set("verb", *msg.Verb)
					record.Set("object", *msg.Object)
					record.Set("data", *data)
					writer.Write(record)
					file.Sync()
				}
			}
		}
	}
	log.Println("Exited MessageLoop")
}

func fileRotator(exitCh chan int) {
	currentHour := time.Now().UTC().Hour()
	prevHour := currentHour - 1
	if prevHour < 0 {
		prevHour = 23
	}

RotateLoop:
	for {

		select {

		case <-exitCh:
			break RotateLoop

		default:

			time.Sleep(30 * time.Second)

			now := time.Now().UTC()
			hour := now.Hour()
			if hour != currentHour {
				closeHour := prevHour
				prevHour = currentHour
				currentHour = hour

				// Kill the goroutine which dumps messages to the temp file
				exitChan[closeHour] <- 1
				log.Println("Waiting for writer to exit")
				<-exitedChan[closeHour]
				log.Println("Finished waiting")

				// Get the date during closeHour (not necessarily the same date as now)
				closeDate := now.Add(-2 * time.Hour)

				// Move the temp file to its permanent place
				filename := fmt.Sprintf("%s%02d", *fileprefix, closeHour)
				tmpFile := filepath.Join(*tmpdir, filename)
				outPath := path.Join(*outdir, fmt.Sprintf("%d", closeDate.Year()), fmt.Sprintf("%02d", closeDate.Month()), fmt.Sprintf("%02d", closeDate.Day()))
				os.MkdirAll(outPath, 0777)
				outFile := filepath.Join(outPath, filename)
				err := os.Rename(tmpFile, outFile)
				if err != nil {
					log.Println(err)
				} else {
					log.Println("Closed " + outFile)
				}

				// Restart the goroutine
				go fileWriter(closeHour)
			}
		}

	}

}
