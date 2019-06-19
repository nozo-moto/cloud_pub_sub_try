package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

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
	pubsubProjectID = os.GetEnv("PUBSUB_PROJECT_ID")
	if pubsubProjectID == "" {
		pubsubProjectID = "jin-infra"
	}

	pubsubTopicID = os.GetEnv("PUBSUB_TOPIC_ID")
	if pubsubTopicID == "" {
		pubsubTopicID = "projects/jin-infra/topics/dev_topic"
	}
}

type Message struct {
	ID   int64  `json:"id"`
	Text string `json:"text"`
}

func main() {
	var err error
	var exists bool
	psClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	topic := psClient.Topic(pubsubTopicID)
	exists, err = topic.Exists()
	if err != nil {
		panic(err)
	}
	if !exists {
		topic, err = psClient.CreateTopic(ctx, pubsubTopicID)
		if err != nil {
			panic(err)
		}
	}

	subscription = psClient.Subscription(subscriptionName)
	exists, err = subscription.Exists()
	if err != nil {
		panic(err)
	}
	if !exists {
		_, err = psClient.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			panic(err)
		}
	}

	// subscribe
	go func() {
		//	subscription.
		for {
			ctx := context.Background()
			err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				var message Message
				if err := json.Unmarshal(msg.Data, &message); err != nil {
					msg.Ack()
					return
				}

				log.Println("message", message.ID, message.Text)
			})

			time.Sleep(time.Second * 1)
		}
	}()

	// publish
	go func() {
		for i := 0; i < 10; i++ {
			ctx := context.Background()
			b, err := json.Marshal(Message{ID: i, Text: "this is test publish."})
			if err != nil {
				panic(err)
			}

			_, err := topic.Publish(ctx, &pubsub.Message{Data: b})
			if err != nil {
				panic(err)
			}
		}
	}()

	for {
	}
}
