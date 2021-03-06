package unbolted

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/boltdb/bolt"
	"github.com/zond/setop"
)

type TX struct {
	tx *bolt.Tx
	db *DB
}

/*
DB returns the DB of this TX.
*/
func (self *TX) DB() *DB {
	return self.db
}

func (self *TX) update(id []byte, oldValue, objValue reflect.Value, typ reflect.Type, obj interface{}) (err error) {
	if updatedAt := objValue.FieldByName(updatedAtField); updatedAt.IsValid() && updatedAt.Type() == timeType {
		updatedAt.Set(reflect.ValueOf(time.Now()))
	}
	if err = self.deIndex(id, oldValue, typ); err != nil {
		return
	}
	if err = self.index(id, objValue, typ); err != nil {
		return
	}
	if err = self.save(id, typ, obj); err != nil {
		return
	}
	if err = self.db.AfterTransaction(func(db *DB) (err error) {
		return db.emit(typ, &oldValue, &objValue)
	}); err != nil {
		return
	}
	return nil
}

func (self *TX) save(id []byte, typ reflect.Type, obj interface{}) (err error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	buckets, err := self.dig([][]byte{primaryKey, []byte(typ.Name())}, true)
	if err != nil {
		return
	}
	return buckets[len(buckets)-1].Put(id, bytes)
}

func (self *TX) create(id []byte, value reflect.Value, typ reflect.Type, obj interface{}) (err error) {
	if updatedAt := value.FieldByName(updatedAtField); updatedAt.IsValid() && updatedAt.Type() == timeType {
		updatedAt.Set(reflect.ValueOf(time.Now()))
	}
	if createdAt := value.FieldByName(createdAtField); createdAt.IsValid() && createdAt.Type() == timeType {
		createdAt.Set(reflect.ValueOf(time.Now()))
	}
	if err := self.index(id, value, typ); err != nil {
		return err
	}
	if err = self.save(id, typ, obj); err != nil {
		return
	}
	if err = self.db.AfterTransaction(func(db *DB) (err error) {
		return db.emit(typ, nil, &value)
	}); err != nil {
		return
	}
	return nil
}

/*
Count returns the number of objects of the same type as obj in this TX.
*/
func (self *TX) Count(obj interface{}) (result int, err error) {
	value, _, err := identify(obj)
	if err != nil {
		return
	}
	buckets, err := self.dig([][]byte{primaryKey, []byte(value.Type().Name())}, false)
	if err != nil {
		if err == ErrNotFound {
			err = nil
		}
		return
	}
	result = buckets[len(buckets)-1].Stats().KeyN
	return
}

func (self *TX) dig(keys [][]byte, create bool) (buckets []*bolt.Bucket, err error) {
	var bucket *bolt.Bucket
	if create {
		if bucket, err = self.tx.CreateBucketIfNotExists(keys[0]); err != nil {
			return
		}
	} else {
		if bucket = self.tx.Bucket(keys[0]); bucket == nil {
			err = ErrNotFound
			return
		}
	}
	buckets = append(buckets, bucket)
	for keys = keys[1:]; len(keys) > 0; keys = keys[1:] {
		if create {
			if bucket, err = bucket.CreateBucketIfNotExists(keys[0]); err != nil {
				return
			}
		} else {
			if bucket = bucket.Bucket(keys[0]); bucket == nil {
				err = ErrNotFound
				return
			}
		}
		buckets = append(buckets, bucket)
	}
	return
}

/*
Index will to load obj in this TX, de-index it and then index it again.
Indexed fields are annotated with `unbolted:"index"`.
*/
func (self *TX) Index(obj interface{}) (err error) {
	value, id, err := identify(obj)
	if err != nil {
		return err
	}
	idBytes := id.Bytes()
	if idBytes == nil {
		return fmt.Errorf("Can't Index %+v without Id", obj)
	}
	typ := value.Type()
	old := reflect.New(typ).Interface()
	oldValue := reflect.ValueOf(old).Elem()
	if err = self.get(idBytes, oldValue, old); err != nil {
		return
	}
	if err = self.deIndex(idBytes, oldValue, oldValue.Type()); err != nil {
		return
	}
	return self.index(idBytes, value, value.Type())
}

func (self *TX) index(id []byte, value reflect.Value, typ reflect.Type) (err error) {
	var indexed [][][]byte
	if indexed, err = indexKeys(id, value, typ); err != nil {
		return
	}
	for _, keys := range indexed {
		var buckets []*bolt.Bucket
		if buckets, err = self.dig(keys[:len(keys)-1], true); err != nil {
			return
		}
		if err = buckets[len(buckets)-1].Put(keys[len(keys)-1], []byte{0}); err != nil {
			return
		}
	}
	return
}

func (self *TX) deIndex(id []byte, value reflect.Value, typ reflect.Type) (err error) {
	var indexed [][][]byte
	if indexed, err = indexKeys(id, value, typ); err != nil {
		return
	}
	for _, keys := range indexed {
		var buckets []*bolt.Bucket
		if buckets, err = self.dig(keys[:len(keys)-1], true); err != nil {
			return
		}
		if err = buckets[len(buckets)-1].Delete(keys[len(keys)-1]); err != nil {
			return
		}
		for ; len(buckets) > 1; buckets = buckets[:len(buckets)-1] {
			stats := buckets[len(buckets)-2].Stats()
			if stats.BucketN > 1 || stats.KeyN > 0 {
				break
			}
			if err = buckets[len(buckets)-2].DeleteBucket(keys[len(buckets)-1]); err != nil {
				return
			}
		}
	}
	return
}

func (self *TX) get(id []byte, value reflect.Value, obj interface{}) (err error) {
	buckets, err := self.dig([][]byte{primaryKey, []byte(value.Type().Name())}, false)
	if err != nil {
		return
	}
	b := buckets[len(buckets)-1].Get(id)
	if b == nil {
		err = ErrNotFound
		return
	}
	return json.Unmarshal(b, obj)
}

/*
Clear will empty this TX.
*/
func (self *TX) Clear() (err error) {
	cursor := self.tx.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		if err = self.tx.DeleteBucket(key); err != nil {
			return
		}
	}
	return
}

/*
Set will save obj in this TX.
If obj has no Id, or if the Id does not already exist in the TX, it will be indexed and created.
If obj has an Id that exists in the TX, the old object will be loaded and de-indexed, then obj will be indexd and saved.
Indexed fields have the annotation `unbolted:"index"`.
*/
func (self *TX) Set(obj interface{}) (err error) {
	value, id, err := identify(obj)
	if err != nil {
		return
	}
	if idBytes := id.Bytes(); idBytes == nil {
		idBytes = randomBytes()
		id.SetBytes(idBytes)
		return self.create(idBytes, value, value.Type(), obj)
	} else {
		typ := value.Type()
		old := reflect.New(typ).Interface()
		oldValue := reflect.ValueOf(old).Elem()
		if err = self.get(idBytes, oldValue, old); err == nil {
			return self.update(idBytes, oldValue, value, typ, obj)
		} else {
			if err != ErrNotFound {
				return
			}
			return self.create(idBytes, value, value.Type(), obj)
		}
	}
}

/*
Get will load the object in this TX of the same type and id as obj into obj.
*/
func (self *TX) Get(obj interface{}) error {
	value, id, err := identify(obj)
	if err != nil {
		return err
	}
	return self.get(id.Bytes(), value, obj)
}

/*
Del will delete the object in this TX of the same type and id as obj.
*/
func (self *TX) Del(obj interface{}) (err error) {
	value, id, err := identify(obj)
	if err != nil {
		return
	}
	typ := value.Type()
	buckets, err := self.dig([][]byte{primaryKey, []byte(typ.Name())}, false)
	if err != nil {
		return
	}
	b := buckets[len(buckets)-1].Get(id.Bytes())
	if b == nil {
		return
	}
	if err = json.Unmarshal(b, obj); err != nil {
		return
	}
	if err = self.deIndex(id.Bytes(), value, typ); err != nil {
		return
	}
	if err = buckets[len(buckets)-1].Delete(id.Bytes()); err != nil {
		return
	}
	if err = self.db.AfterTransaction(func(db *DB) (err error) {
		return db.emit(typ, &value, nil)
	}); err != nil {
		return
	}
	return
}

func (self *TX) skipper(b []byte) (result setop.Skipper, err error) {
	keys := splitKeys(b)
	buckets, err := self.dig(keys, false)
	if err != nil {
		if err == ErrNotFound {
			err = nil
			result = &skipper{}
		}
		return
	}
	result = &skipper{
		cursor: buckets[len(buckets)-1].Cursor(),
	}
	return
}

func (self *TX) setOp(expr *setop.SetExpression) (result []kv) {
	if err := expr.Each(self.skipper, func(res *setop.SetOpResult) {
		result = append(result, kv{
			Keys:  [][]byte{res.Key},
			Value: res.Values[0],
		})
	}); err != nil {
		panic(err)
	}
	return
}

/*
Query will return a query in this TX.
*/
func (self *TX) Query() *Query {
	return &Query{
		db: self.db,
		run: func(f func(*TX) error) error {
			return f(self)
		},
	}
}
