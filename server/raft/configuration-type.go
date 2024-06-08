package raft

// ServerConfigurationStatus determine whether a Server in a NodeConfiguration get a vote
type ServerConfigurationStatus int

const (
	Voter ServerConfigurationStatus = 0
	// Nonvoter is a server that receives log entries but is not considered
	// for election and commitment purposes
	Nonvoter ServerConfigurationStatus = 1
)

func (s ServerConfigurationStatus) String() string {
	switch s {
	case Voter:
		return "Voter"
	case Nonvoter:
		return "Nonvoter"
	}

	return "ServerConfigurationStatus"
}

type ConfigurationChangeCommand int

const (
	AddVoter     ConfigurationChangeCommand = 0
	AddNonvoter  ConfigurationChangeCommand = 1
	DemoteVoter  ConfigurationChangeCommand = 2
	RemoveServer ConfigurationChangeCommand = 3
)

func (c ConfigurationChangeCommand) String() string {
	switch c {
	case AddVoter:
		return "AddVoter"
	case AddNonvoter:
		return "AddNonvoter"
	case DemoteVoter:
		return "DemoteVoter"
	case RemoveServer:
		return "RemoveServer"
	}
	return "ConfigurationChangeCommand"
}
