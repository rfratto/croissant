package api

// Health state for a node.
type Health uint

const (
	// Healthy is the default state.
	Healthy Health = iota
	// Unhealthy represnts a suspicion that a node is misbehaving.
	Unhealthy
	// Dead represents a dead node that should be removed from the state.
	Dead
)

// String returns the health as a string.
func (h Health) String() string {
	switch h {
	case Healthy:
		return "Healthy"
	case Unhealthy:
		return "Unhealthy"
	case Dead:
		return "Dead"
	default:
		return "Unknown"
	}
}
