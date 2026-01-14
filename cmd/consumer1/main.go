package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rkohnovets/kafka-course-unit1-practice2/cmd/shared"
)

// SingleMessageConsumer

func main() {
	// задержка чтобы
	// 1) успел запуститься кластер kafka
	// 2) успел создасться топик (см. docker-compose.yml)
	time.Sleep(time.Second * 30)

	topic := shared.TopicName
	brokers := shared.BootstrapServers
	consumerGroup := "single-consumer-group"

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          consumerGroup,

		"auto.offset.reset": "earliest",

		"enable.auto.commit":      true,
		"auto.commit.interval.ms": 1000,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Consumer subscribe failed: %v", err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	consume := true
	for consume {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			consume = false
		default:
			ev, err := consumer.ReadMessage(time.Millisecond * 100)
			if err != nil {
				if !err.(kafka.Error).IsTimeout() {
					log.Printf("ReadMessage error: %v\n", err)
				}
				continue
			}

			msg := shared.MyMessage{}
			err = json.Unmarshal(ev.Value, &msg)
			if err != nil {
				log.Printf("Unmarshaling message error: %v\n", err)
				continue
			}

			log.Printf(
				"Received message: time=%v topic=%s partition=%d offset=%v key=%s value=%v\n\n",
				time.Now(),
				*ev.TopicPartition.Topic,
				ev.TopicPartition.Partition,
				ev.TopicPartition.Offset,
				string(ev.Key),
				msg,
			)
		}
	}

	log.Println("Closing consumer...")
}
