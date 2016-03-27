package main

import (
	"bytes"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func CreateQueue(ch *amqp.Channel, queueName string, params amqp.Table) amqp.Queue {
	q, err := ch.QueueDeclare(
		queueName, // queue name
		true,      // durable
		false,     // delete when unused
		false,     // ixclusive
		false,     // no-wait
		params,    // optional args
	)
	failOnError("Fail to declare a queue", err)

	return q
}

func getConsumeChan(ch *amqp.Channel, q amqp.Queue, params amqp.Table) <-chan amqp.Delivery {
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		params, // args
	)
	failOnError("Failed to register a consumer", err)

	return msgs
}

func failOnError(message string, err error) {
	if err != nil {
		log.Panicf("%s: %s", message, err.Error())
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError("Failed to connect to RabbitMQ", err)
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError("Failed to open a channel", err)
	defer ch.Close()

	helloQueue := CreateQueue(ch, "helloQueue", nil)
	msgs := getConsumeChan(ch, helloQueue, nil)

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError("Failed on setting QoS", err)

	forever := make(chan bool)
	go func() {
		for m := range msgs {
			log.Printf("Received a message: %s", m.Body)
			m.Ack(false)
			dot_count := bytes.Count(m.Body, []byte("."))
			t := time.Duration(dot_count)
			time.Sleep(t * time.Second)
			log.Printf("Done")
		}
	}()

	<-forever
}
