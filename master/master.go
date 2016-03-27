package main

import (
	"log"
	"os"
	"strings"

	"github.com/streadway/amqp"
)

func failOnError(message string, err error) {
	if err != nil {
		log.Panicf("%s: %s", message, err.Error())
	}
}

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

func publishMessage(ch *amqp.Channel, q amqp.Queue, message string) {
	err := ch.Publish(
		"",     // exchange
		q.Name, // routing Name
		false,  //mandatory
		false,  //immidiate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(message),
		})
	failOnError("Fail to publish message", err)
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	failOnError("Fail to connect to rabbitMQ server", err)
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError("Fail to open a channel", err)
	defer ch.Close()

	helloQueue := CreateQueue(ch, "helloQueue", nil)
	publishMessage(ch, helloQueue, bodyFrom(os.Args))
}
