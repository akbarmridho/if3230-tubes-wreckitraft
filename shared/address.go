package shared

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ParseAddressError = errors.New("Address format is invalid")

type Address struct {
	IP   string
	Port int
}

func (a Address) Equals(other Address) bool {
	return a.IP == other.IP && a.Port == other.Port
}

func (a Address) Host() string {
	return fmt.Sprintf("%s:%d", a.IP, a.Port)
}

func StringToAddress(data string) (*Address, error) {
	splitted := strings.Split(data, ":")

	if len(splitted) != 2 {
		return nil, ParseAddressError
	}

	port, err := strconv.ParseInt(splitted[1], 10, 32)

	if err != nil {
		return nil, ParseAddressError
	}

	result := Address{
		IP:   splitted[0],
		Port: int(port),
	}

	return &result, nil
}
