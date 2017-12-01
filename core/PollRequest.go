package core

import "github.com/golang-plus/uuid"

type PollRequest struct {
	Id string
}

func NewRequest() (*PollRequest, error) {
	id, err := uuid.NewV4()

	if (err != nil) {
		return nil, err
	}

	return &PollRequest{Id: id.String()}, nil
}
