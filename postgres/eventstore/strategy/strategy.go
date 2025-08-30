package strategy

import (
	"errors"
	"fmt"
	"regexp"
)

func validateTableName(name string) error {
	if ok, err := regexp.Match(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`, []byte(name)); err != nil {
		return fmt.Errorf("validating table name: %w", err)
	} else if !ok {
		return errors.New("table name must be a valid SQL identifier")
	}

	return nil
}
