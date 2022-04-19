package common

import "fmt"

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
