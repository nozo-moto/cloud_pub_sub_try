package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
)

var (
	pubsubTopicID   string
	pubsubProjectID string
)

const (
	subscriptionName = "Subscription_name"
)

func init() {
	pubsubProjectID = os.Getenv("PUBSUB_PROJECT_ID")
	if pubsubProjectID == "" {
		pubsubProjectID = "<>"
	}

	pubsubTopicID = os.Getenv("PUBSUB_TOPIC_ID")
	if pubsubTopicID == "" {
		pubsubTopicID = "<>"
	}
}

type Message struct {
	ID   int64  `json:"id"`
	Text string `json:"text"`
}

func main() {
	var err error
	var exists bool

	ctx := context.Background()
	psClient, err := pubsub.NewClient(ctx, pubsubProjectID)
	if err != nil {
		panic(err)
	}

	topic := psClient.Topic(pubsubTopicID)
	exists, err = topic.Exists(ctx)
	if err != nil {
		panic(err)
	}
	if !exists {
		topic, err = psClient.CreateTopic(ctx, pubsubTopicID)
		if err != nil {
			panic(err)
		}
	}

	subscription := psClient.Subscription(subscriptionName)
	exists, err = subscription.Exists(ctx)
	if err != nil {
		panic(err)
	}
	if !exists {
		_, err = psClient.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			panic(err)
		}
	}

	log.Println("Finished Init")

	// subscribe
	go func() {
		log.Println("Run Subscription")
		for {
			ctx := context.Background()
			err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				var message Message
				if err := json.Unmarshal(msg.Data, &message); err != nil {
					msg.Ack()
					return
				}

				log.Println("subscription", message.ID, message.Text)

				msg.Ack()
			})
			if err != nil {
				panic(err)
			}

		}
	}()

	// publish
	go func() {
		log.Println("Run Publisher")
		for i := 0; i < 10; i++ {
			ctx := context.Background()
			message := Message{ID: int64(i), Text: "this is test publish."}
			b, err := json.Marshal(message)
			if err != nil {
				panic(err)
			}

			_, err = topic.Publish(ctx, &pubsub.Message{Data: b}).Get(ctx)
			if err != nil {
				panic(err)
			}
			log.Println("publish", message.ID, message.Text)
		}
	}()

	for {
	}
}
