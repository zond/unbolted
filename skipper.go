package unbolted

import (
	"bytes"

	"github.com/boltdb/bolt"
	"github.com/zond/setop"
)

type KV struct {
	Keys  [][]byte
	Value []byte
}

type skipper struct {
	cursor    *bolt.Cursor
	lastKey   []byte
	lastValue []byte
}

// Skip returns a value matching the min and inclusive criteria.
// If the last yielded value matches the criteria the same value will be returned again.
func (self *skipper) Skip(min []byte, inc bool) (result *setop.SetOpResult, err error) {
	if self.cursor == nil {
		return
	}
	var key []byte
	var value []byte

	if self.lastKey == nil {
		if min == nil {
			key, value = self.cursor.First()
		} else {
			key, value = self.cursor.Seek(min)
		}
	} else {
		if min == nil {
			key, value = self.lastKey, self.lastValue
		} else if bytes.Compare(min, self.lastKey) < 1 {
			key, value = self.lastKey, self.lastValue
		} else {
			key, value = self.cursor.Seek(min)
		}
	}

	if !inc && min != nil && bytes.Compare(min, key) == 0 {
		key, value = self.cursor.Next()
	}

	self.lastKey, self.lastValue = key, value

	if key != nil {
		result = &setop.SetOpResult{
			Key:    key,
			Values: [][]byte{value},
		}
	}

	return
}
