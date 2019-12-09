package controllers

import (
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go/service/sqs"
)

//define msgsqs
type MsgSqs struct {
	Message int `json:"message"`
}

func ReadMsg(msg *sqs.Message) error {

	var data MsgSqs
	//mengubah message sqs yang berformat json menjadi data
	err := json.Unmarshal([]byte(*msg.Body), &data)
	if err != nil {
		return err
	}
	log.Printf("[DATA : %d]\n", data.Message)

	return nil

}
