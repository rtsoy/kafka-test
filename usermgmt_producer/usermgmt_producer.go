package main

import (
	"encoding/json"
	"fmt"

	"kafka-test/usermgmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	newUsers := map[string]int{
		"Alice": 32,
		"Mark":  17,
		"John":  25,
		"Marry": 20,
	}

	topic := usermgmt.UserManagementTopic

	for name, age := range newUsers {
		user := usermgmt.User{
			Name: name,
			Age:  age,
		}

		userBytes, err := json.Marshal(user)
		if err != nil {
			fmt.Printf("Failed to marshal a user to JSON: %v", err)
			continue
		}

		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          userBytes,
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
