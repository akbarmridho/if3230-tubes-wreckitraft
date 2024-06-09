package shared

import "fmt"

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
