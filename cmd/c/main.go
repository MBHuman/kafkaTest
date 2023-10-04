package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

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

	flag.StringVar(&brokers, "borkers", os.Getenv("KAFKA_BORKER"), "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&topic, "topic", os.Getenv("KAFKA_TOPIC"), "Kafka topic to be consumed")
	flag.StringVar(&username, "username", os.Getenv("KAFKA_USER"), "username to connect to broker")
	flag.StringVar(&password, "password", os.Getenv("KAFKA_PASSWORD"), "password to connect to broker")

	// SASL configuration
	config := sarama.NewConfig()
	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512

	// Create a Kafka consumer
	consumer, err := sarama.NewConsumer(strings.Split(brokers, ","), config)
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}
	defer consumer.Close()

	// Subscribe to the topic
	// consumerGroup := "my-consumer-group" // Change to your desired consumer group
	// topics := []string{topic}
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("Error creating partition consumer: %v\n", err)
		return
	}
	defer partitionConsumer.Close()

	// Trap signals to gracefully shut down the consumer
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Consume messages
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message: Key = %s, Value = %s\n", msg.Key, msg.Value)
		case err := <-partitionConsumer.Errors():
			fmt.Printf("Error: %v\n", err)
		case <-signals:
			fmt.Println("Received interrupt. Shutting down...")
			return
		}
	}
}
