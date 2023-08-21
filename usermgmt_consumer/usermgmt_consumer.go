package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"kafka-test/usermgmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	dbName   = "usermgmt"
	collName = "users"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("failed to create a new customer: %v", err)
	}

	mongo, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("failed to connect to mongodb: %v", err)
	}
	db := mongo.Database(dbName).Collection(collName)

	c.SubscribeTopics([]string{usermgmt.UserManagementTopic}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			var user usermgmt.User
			if err := json.Unmarshal(msg.Value, &user); err != nil {
				fmt.Printf("Failed to unmarshal a user: %v\n", err)
				continue
			}

			if res, err := db.InsertOne(context.Background(), user); err != nil {
				fmt.Printf("Failed to insert a user to db: %v\n", err)
				continue
			} else {
				user.ID = res.InsertedID.(primitive.ObjectID)
			}

			fmt.Printf("New user: %s\n", user.ID.Hex())

		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
