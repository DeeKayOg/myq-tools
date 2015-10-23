package myqlib

// MyqState contains the current and previous SHOW STATUS outputs.  Also SHOW VARIABLES.
// Prev might be nil
type MyqState struct {
	Cur, Prev   MyqSample
	SecondsDiff float64 // Difference between Cur and Prev
	FirstUptime int64   // Uptime of our first sample this run
}

func NewMyqState() MyqState {
	return MyqState{NewMyqSample(),NewMyqSample(), 0, 0}
}
