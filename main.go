package main

import (
	"bufio"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// connect
	log.Info("connecting to RabbitMQ")
	conn, err := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
	failOnError(err, "failed to connect to RabbitMQ")
	defer conn.Close()
	log.Info("connect to RabbitMQ success")

	//=========== sender ===========
	// open a sender channel
	chSender, err := conn.Channel()
	failOnError(err, "failed to open a sender channel")
	defer chSender.Close()
	log.Info("open sender channel success")

	// declare a sender queue
	qSender, err := chSender.QueueDeclare(
		"neuoj",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare a sender queue")
	log.Infof("declare sender queue with name %s success", qSender.Name)

	//=========== consumer ===========
	// open a consumer channel
	chConsumer, err := conn.Channel()
	failOnError(err, "failed to open a consumer channel")
	defer chConsumer.Close()
	log.Info("open sender consumer success")

	// declare a consumer queue
	qConsumer, err := chConsumer.QueueDeclare(
		"neuoj",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare a consumer queue")
	log.Infof("declare consumer queue with name %s success", qConsumer.Name)

	// register consumer
	msgs, err := chConsumer.Consume(
		qConsumer.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")
	log.Info("register consumer success")

	go func() {
		for msg := range msgs {
			log.Infof("received a message: %s", msg.Body)
			time.Sleep(2 * time.Second)
			_ = msg.Ack(false)
		}
	}()

	// test publish message
	reader := bufio.NewReader(os.Stdin)
	for {
		body, err := reader.ReadString('\n')
		failOnError(err, "failed to read text from keyboard")
		msg := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		}
		err = chSender.Publish(
			"",
			qSender.Name,
			false,
			false,
			msg,
		)
		failOnError(err, "failed to publish a message")
		log.Infof("publish message with content %v success", msg.Body)
	}
}
