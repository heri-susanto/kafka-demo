package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:29092", "localhost:39092"},
		GroupID:  "consumer-group-1",
		Topic:    "test_topic",
		// MinBytes: 10e3, // 10KB
		// MaxBytes: 10e6, // 10MB
	})
}

func main() {
	reader := getKafkaReader()
	defer reader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
