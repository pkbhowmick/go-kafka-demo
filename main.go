package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

var (
	KafkaBrokerURLs = []string{"localhost:9093"}
	KafkaTopic      = "Person"
)

type Person struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func connectProducer(brokerURL []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokerURL, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer: %v", err.Error())
	}
	return conn, nil
}

func pushDataToQueue(topic string, message []byte) error {
	producer, err := connectProducer(KafkaBrokerURLs)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %v", err.Error())
	}
	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func connectConsumer(brokerURL []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	conn, err := sarama.NewConsumer(brokerURL, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func startConsumer(doneCh chan struct{}, sigCh chan os.Signal, consumer sarama.PartitionConsumer) {
	for {
		select {
		case errs := <-consumer.Errors():
			log.Println(errs)
		case msg := <-consumer.Messages():
			log.Printf("Received message: Topic (%s) | Message (%s)", msg.Topic, string(msg.Value))
		case <-sigCh:
			log.Println("Stopping consumer")
			doneCh <- struct{}{}
		}
	}
}

func main() {
	person := &Person{
		Name:  "Pulak Kanti Bhowmick",
		Email: "pkbhowmick007@gmail.com",
	}

	data, err := json.Marshal(person)
	if err != nil {
		panic(err)
	}

	err = pushDataToQueue(KafkaTopic, data)
	if err != nil {
		panic(err)
	}
	log.Println("Successfully sent the data in the queue")

	worker, err := connectConsumer(KafkaBrokerURLs)
	if err != nil {
		panic(err)
	}
	defer worker.Close()

	consumer, err := worker.ConsumePartition(KafkaTopic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	doneCh := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go startConsumer(doneCh, sigChan, consumer)
	<-doneCh
}
