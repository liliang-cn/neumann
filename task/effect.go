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

// MarshalText stores an Effect by name, so stored call records do not depend
// on the order of the constants.
func (e Effect) MarshalText() ([]byte, error) { return nameOf("Effect", int(e), effectNames[:]) }

func (e *Effect) UnmarshalText(b []byte) error {
	i, err := indexOf("Effect", b, effectNames[:])
	if err == nil {
		*e = Effect(i)
	}
	return err
}

// Valid reports whether e is one of the declared effects.
func (e Effect) Valid() bool { return e >= 0 && int(e) < len(effectNames) }
