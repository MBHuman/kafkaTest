package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"crypto/sha256"
	"crypto/sha512"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
	"github.com/xdg-go/scram"
)

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New

	brokers  string
	topic    string
	username string
	password string
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

func main() {
	if err := godotenv.Load(); err != nil {
		panic("Filed to load env")
	}

	// Define Kafka broker addresses and the topic to produce to.
	flag.StringVar(&brokers, "borkers", os.Getenv("KAFKA_BORKER"), "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&topic, "topic", os.Getenv("KAFKA_TOPIC"), "Kafka topic to be produced")
	flag.StringVar(&username, "username", os.Getenv("KAFKA_USER"), "username to connect to broker")
	flag.StringVar(&password, "password", os.Getenv("KAFKA_PASSWORD"), "password to connect to broker")

	// Set up SASL SCRAM authentication
	config := sarama.NewConfig()
	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password
	config.Net.SASL.Handshake = true
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512

	config.Producer.Return.Successes = true

	// Create a Kafka producer
	producer, err := sarama.NewSyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}
	defer producer.Close()

	fmt.Println("Before message")

	// Produce a message to Kafka.
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Hello, Kafka!"),
	}

	fmt.Println("After message")

	// Send the message to Kafka
	_, _, err = producer.SendMessage(message)
	if err != nil {
		log.Printf("Error producing message: %v", err)
		// Handle the error gracefully here
		return
	}

	fmt.Println("Message produced to Kafka.")
}
