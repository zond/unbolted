package unbolted

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"
)

const (
	unbolted       = "unbolted"
	idField        = "Id"
	index          = "index"
	updatedAtField = "UpdatedAt"
	createdAtField = "CreatedAt"
)

/*
Id is what identifies objects.
*/
type Id []byte

func (self Id) String() string {
	return base64.URLEncoding.EncodeToString(self)
}

/*
DecodeId decodes the String() representation of an Id.
*/
func DecodeId(s string) (result Id, err error) {
	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return
	}
	result = Id(b)
	return
}

func (self Id) MarshalJSON() (b []byte, err error) {
	return json.Marshal(self.String())
}

func (self *Id) UnmarshalJSON(b []byte) (err error) {
	base64Encoded := ""
	if err = json.Unmarshal(b, &base64Encoded); err != nil {
		return
	}
	*self, err = base64.URLEncoding.DecodeString(base64Encoded)
	return
}

func (self *Id) Equals(o Id) bool {
	return bytes.Compare([]byte(*self), []byte(o)) == 0
}

var primaryKey = []byte("pk")
var secondaryIndex = []byte("2i")
var timeType = reflect.TypeOf(time.Now())
var ErrNotFound = fmt.Errorf("Not found")

func identify(obj interface{}) (value, id reflect.Value, err error) {
	ptrValue := reflect.ValueOf(obj)
	if ptrValue.Kind() != reflect.Ptr {
		err = fmt.Errorf("%v is not a pointer", obj)
		return
	}
	value = ptrValue.Elem()
	if value.Kind() != reflect.Struct {
		err = fmt.Errorf("%v is not a pointer to a struct", obj)
		return
	}
	id = value.FieldByName(idField)
	if id.Kind() == reflect.Invalid {
		err = fmt.Errorf("%v does not have an Id field", obj)
		return
	}
	if !id.CanSet() {
		err = fmt.Errorf("%v can not assign its Id field", obj)
		return
	}
	if id.Kind() != reflect.Slice {
		err = fmt.Errorf("%v does not have a byte slice Id field", obj)
	}
	if id.Type().Elem().Kind() != reflect.Uint8 {
		err = fmt.Errorf("%v does not have a byte slice Id field", obj)
	}
	return
}

func randomBytes() (result []byte) {
	result = make([]byte, 24)
	for i, _ := range result {
		result[i] = byte(rand.Int())
	}
	return
}

func indexBytes(typ reflect.Type, value reflect.Value) (b []byte, err error) {
	switch typ.Kind() {
	case reflect.String:
		b = []byte(value.String())
	case reflect.Int:
		buf := new(bytes.Buffer)
		if err = binary.Write(buf, binary.BigEndian, value.Int()); err != nil {
			return
		}
		b = buf.Bytes()
	case reflect.Slice:
		switch typ.Elem().Kind() {
		case reflect.Uint8:
			b = value.Bytes()
		default:
			err = fmt.Errorf("%v is not an indexable type", typ)
		}
	case reflect.Bool:
		if value.Bool() {
			b = []byte{1}
		} else {
			b = []byte{0}
		}
	default:
		err = fmt.Errorf("%v is not an indexable type", typ)
		return
	}
	if len(b) == 0 {
		b = []byte{0}
	}
	return
}

func indexKey(id []byte, typ reflect.Type, fieldName string, fieldType reflect.Type, fieldValue reflect.Value) (keys [][]byte, err error) {
	var valuePart []byte
	if valuePart, err = indexBytes(fieldType, fieldValue); err != nil {
		return
	}
	keys = [][]byte{
		secondaryIndex,
		[]byte(typ.Name()),
		[]byte(fieldName),
		valuePart,
		id,
	}
	return
}

func indexKeys(id []byte, value reflect.Value, typ reflect.Type) (indexed [][][]byte, err error) {
	alreadyIndexed := make(map[string]bool)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if unboltedTag := field.Tag.Get(unbolted); unboltedTag != "" {
			for _, param := range strings.Split(unboltedTag, ",") {
				if param == index {
					// unbolted:"index"
					if !alreadyIndexed[field.Name] {
						// Not already indexed
						var keys [][]byte
						// Build an index key
						keys, err = indexKey(id, typ, field.Name, field.Type, value.Field(i))
						if err != nil {
							return
						}
						indexed = append(indexed, keys)
						alreadyIndexed[field.Name] = true
					}
				}
			}
		}
	}
	return
}

func escape(bs []byte) (result []byte) {
	for index := 0; index < len(bs); index++ {
		if bs[index] == 0 {
			result = append(result, 0, 0)
		} else {
			result = append(result, bs[index])
		}
	}
	result = append(result, 0, 1)
	return
}

func joinKeys(keys [][]byte) (result []byte) {
	for _, key := range keys {
		result = append(result, escape(key)...)
	}
	return
}

func splitKeys(key []byte) (result [][]byte) {
	var last []byte
	for index := 0; index < len(key); index++ {
		if key[index] == 0 {
			if key[index+1] == 1 {
				result = append(result, last)
				last = nil
			} else {
				last = append(last, 0)
			}
			index++
		} else {
			last = append(last, key[index])
		}
	}
	return
}

func minimum(result int, slice ...int) int {
	for _, i := range slice {
		if i < result {
			result = i
		}
	}
	return result
}

func callErr(f reflect.Value, args []reflect.Value) (err error) {
	res := f.Call(args)
	if len(res) > 0 {
		if e, ok := res[len(res)-1].Interface().(error); ok {
			if !res[len(res)-1].IsNil() {
				err = e
				return
			}
		}
	}
	return
}
