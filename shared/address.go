package shared

type Address struct {
	IP      string
	Port    int
	IsHTTPS bool
}

func (a Address) Equals(other Address) bool {
	return a.IP == other.IP && a.Port == other.Port
}
