package redbytes

// Doer is a lightweight subset of the redigo/redis.Conn interface
type Doer interface {
	Do(commandName string, args ...interface{}) (reply interface{}, err error)
}
