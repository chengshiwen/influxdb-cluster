package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

var (
	ErrPromptNotYes = errors.New("prompt not yes")
)

type Error struct {
	Err string `json:"error"`
}

func DecodeError(body io.ReadCloser) error {
	e := &Error{}
	if err := json.NewDecoder(body).Decode(e); err != nil {
		return err
	}
	return errors.New(e.Err)
}

func OperationExitedError(err error) error {
	if err != nil {
		return fmt.Errorf("operation exited with error: %s", err)
	}
	return nil
}

func OperationTimedOutError(err error) error {
	if err != nil {
		return fmt.Errorf("operation timed out with error: %s", err)
	}
	return nil
}
