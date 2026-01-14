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

// BatchMessageConsumer

func main() {
	// задержка чтобы
	// 1) успел запуститься кластер kafka
	// 2) успел создасться топик (см. docker-compose.yml)
	time.Sleep(time.Second * 30)

	topic := shared.TopicName
	brokers := shared.BootstrapServers
	consumerGroup := "batch-consumer-group"

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          consumerGroup,

		"auto.offset.reset": "earliest",

		"enable.auto.commit": false,

		"fetch.min.bytes": 10 * 100,         // 10 * примерный вес одного сообщения
		"fetch.max.bytes": 50 * 1024 * 1024, // 50 мб

		"fetch.wait.max.ms": 10 * 1000, // 10 секунд
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Consumer subscribe failed: %s", err.Error())
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
					log.Printf("Waiting...\n")
				} else {
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
