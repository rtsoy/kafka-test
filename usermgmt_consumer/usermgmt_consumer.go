package main

import (
	"encoding/json"
	"fmt"
	"time"

	"kafka-test/usermgmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{usermgmt.UserManagementTopic}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			var user usermgmt.User
			if err := json.Unmarshal(msg.Value, &user); err != nil {
				fmt.Printf("Failed to unmarshal a user: %v", err)
				continue
			}

			// Add to DB

		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
