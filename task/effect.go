package task

// Effect is what calling a tool does to the world; recovery after a crash
// depends on it. The zero value is SideEffect: an undeclared tool is assumed
// to have one, so a crash makes the machine stop and ask rather than repeat it.
type Effect int

const (
	SideEffect Effect = iota
	ReadOnly
	Idempotent
)

var effectNames = [...]string{"side_effect", "read_only", "idempotent"}

func (e Effect) String() string {
	if e < 0 || int(e) >= len(effectNames) {
		return "unknown"
	}
	return effectNames[e]
}
