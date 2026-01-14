package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rkohnovets/kafka-course-unit1-practice2/cmd/shared"
)

func main() {
	// задержка чтобы
	// 1) успел запуститься кластер kafka
	// 2) успел создасться топик (см. docker-compose.yml)
	time.Sleep(time.Second * 30)

	brokers := shared.BootstrapServers
	topic := shared.TopicName

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",

		"retries":          3,
		"retry.backoff.ms": 100,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// канал для graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// горутина для получения delivery reports
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					log.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	msgCount := 0
	for {
		msgCount++

		select {
		case <-sigchan:
			log.Println("Interrupt received, stopping...")
			return
		case <-ticker.C:
			key := fmt.Sprintf("key-%d", msgCount)

			msg := shared.MyMessage{
				Id:   int64(msgCount),
				Data: fmt.Sprintf("hello from producer, message #%d", msgCount),
			}
			jsonStr, err := json.Marshal(msg)
			if err != nil {
				log.Printf("Cant marshal message: %v\n", err)
				continue
			}

			err = producer.Produce(
				&kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &topic,
						Partition: kafka.PartitionAny,
					},
					Key:   []byte(key),
					Value: []byte(jsonStr),
				},
				nil,
			)
			if err != nil {
				log.Printf("Produce failed: %v\n", err)
			}
		}
	}
}
