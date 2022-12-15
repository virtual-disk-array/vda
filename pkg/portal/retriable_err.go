package portal

type retriableError struct {
	msg string
}

func (e retriableError) Error() string {
	return e.msg
}
