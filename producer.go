package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

func newKafkaWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP("localhost:29092", "localhost:39092"),
		Topic:    "test_topic",
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	writer := newKafkaWriter()
	defer writer.Close()
	fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(fmt.Sprint(uuid.New())),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("produced", key)
		}
		time.Sleep(1 * time.Second)
	}
}
