package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	kafka "github.com/segmentio/kafka-go"
)

func createOrder(w http.ResponseWriter, r *http.Request) {

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatalln(err)
	}

	kafkaWriter := getKafkaWriter()
	defer kafkaWriter.Close()

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("address-%s", r.RemoteAddr)),
		Value: body,
	}
	err = kafkaWriter.WriteMessages(r.Context(), msg)

	if err != nil {
		w.Write([]byte(err.Error()))
		log.Fatalln(err)
	}

	w.Write([]byte("sucess"))

}

func getKafkaWriter() *kafka.Writer {
	kafkaURL := os.Getenv("KAFKA_URL")
	topic := os.Getenv("TOPIC_NAME")
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {

	fmt.Println("start producer-api ... !!")
	http.HandleFunc("/order", createOrder)
	http.ListenAndServe(":8080", nil)
}
