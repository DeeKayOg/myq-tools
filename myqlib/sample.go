package myqlib

import (
	"errors"
	"strconv"
)

// MyqSamples are K->V maps
type MyqSampleMap map[string]string

type MyqSample struct {
	kvs map[string]string
	err error
}

func NewMyqSample() MyqSample {
	return MyqSample{make(map[string]string), nil}
}

// Number of keys in the sample
func (s MyqSample) Length() int {
	return len(s.kvs)
}

func (s MyqSample) Has(key string) bool {
	_, ok := s.kvs[key]
	return ok
}

func (s MyqSample) Get(key string) string {
	return s.kvs[key]
}

func (s MyqSample) Set(key, value string) {
	s.kvs[key] = value
}

func (s *MyqSample) SetError(err error) {
	s.err = err
}

func (s MyqSample) HasError() bool {
	return s.err != nil
}

func (s MyqSample) GetError() error {
	return s.err
}

func (s MyqSample) ForEach(f func(k, v string)) {
	for k, v := range s.kvs {
		f(k, v)
	}
}

// Get methods for the given key. Returns a value of the appropriate type (error is nil) or default value and an error if it can't parse
func (s MyqSample) GetInt(key string) (int64, error) {
	val, ok := s.kvs[key]
	if !ok {
		return 0, errors.New("Key not found")
	}

	conv, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	} else {
		return conv, nil
	}
}
func (s MyqSample) GetFloat(key string) (float64, error) {
	val, ok := s.kvs[key]
	if !ok {
		return 0.0, errors.New("Key not found")
	}

	conv, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0.0, err
	} else {
		return conv, nil
	}
}
func (s MyqSample) GetString(key string) (string, error) {
	val, ok := s.kvs[key]
	if !ok {
		return "", errors.New("Key not found")
	}
	return val, nil // no errors possible here
}

// Same as above, just ignore the error
func (s MyqSample) GetI(key string) int64 {
	i, _ := s.GetInt(key)
	return i
}
func (s MyqSample) GetF(key string) float64 {
	f, _ := s.GetFloat(key)
	return f
}
func (s MyqSample) GetStr(key string) string {
	str, _ := s.GetString(key)
	return str
}

// Gets either a float or an int (check type of result), or an error
func (s MyqSample) GetNumeric(key string) (interface{}, error) {
	if val, err := s.GetInt(key); err != nil {
		return val, nil
	} else if val, err := s.GetFloat(key); err != nil {
		return val, nil
	} else {
		return nil, errors.New("Value is not numeric")
	}
}
