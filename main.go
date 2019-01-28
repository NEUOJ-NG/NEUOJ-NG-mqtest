package main

import (
	"bufio"
	"fmt"
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

func setupTestSender(conn *amqp.Connection) (*amqp.Channel, amqp.Queue) {
	//=========== sender ===========
	// open a sender channel
	chSender, err := conn.Channel()
	failOnError(err, "failed to open a sender channel")
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

	return chSender, qSender
}

func startTestConsumer(conn *amqp.Connection) *amqp.Channel {
	//=========== consumer ===========
	// open a consumer channel
	chConsumer, err := conn.Channel()
	failOnError(err, "failed to open a consumer channel")
	log.Info("open consumer success")

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
	failOnError(err, "failed to register a consumer")
	log.Info("register consumer success")

	go func() {
		for {
			select {
			case msg := <-msgs:
				log.Infof("received a message: %s", msg.Body)
				_ = msg.Ack(false)
			default:
				log.Info("no message in channel")
			}
			time.Sleep(1 * time.Second)
		}
	}()

	return chConsumer
}

func main() {
	log.SetLevel(log.DebugLevel)
	reader := bufio.NewReader(os.Stdin)

	// connect
	log.Info("connecting to RabbitMQ")
	conn, err := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
	failOnError(err, "failed to connect to RabbitMQ")
	defer conn.Close()
	log.Info("connect to RabbitMQ success")

	// setup sender
	chSender, qSender := setupTestSender(conn)
	defer chSender.Close()

	// start consumer on need
	fmt.Print("start consumer? (y/N)")
	ans, _ := reader.ReadString('\n')
	if ans == "Y\n" || ans == "y\n" {
		chConsumer := startTestConsumer(conn)
		defer chConsumer.Close()
	}

	// test publish message
	for {
		body, err := reader.ReadString('\n')
		failOnError(err, "failed to read text from keyboard")
		msg := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
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
