package unbolted

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"

	"github.com/boltdb/bolt"
)

/*
DB wraps a bolt database in an object API.
*/
type DB struct {
	db               *bolt.DB
	lock             sync.RWMutex
	subscriptions    map[string]map[string]*Subscription
	afterTransaction []func(*DB) error
}

func (self *DB) String() string {
	return fmt.Sprintf("&bobject.DB@%p{db:%v,subscriptions:%v}", self, self.db, self.subscriptions)
}

/*
MustDB returns a DB or panics.
*/
func MustDB(path string) (result *DB) {
	result, err := NewDB(path)
	if err != nil {
		panic(err)
	}
	return
}

/*
NewDB returns a DB using (or reusing) path as persistent file.
*/
func NewDB(path string) (result *DB, err error) {
	result = &DB{
		subscriptions: make(map[string]map[string]*Subscription),
	}
	if result.db, err = bolt.Open(path, 0600, nil); err != nil {
		return
	}
	return
}

/*
Close closes the database persistence file.
*/
func (self *DB) Close() (err error) {
	return self.db.Close()
}

/*
AfterTransaction will append f to a list of functions that will run after the current transaction finishes.
If run outside a transaction it will wait until the next transaction finishes.
If f returns an error, the transaction call (Update or View) will return an error, but mutating transactions will still commit!
*/
func (self *DB) AfterTransaction(f func(*DB) error) (err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.afterTransaction = append(self.afterTransaction, f)
	return
}

func (self *DB) runAfterTransaction() (err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for len(self.afterTransaction) > 0 {
		next := self.afterTransaction[0]
		self.afterTransaction = self.afterTransaction[1:]
		if err = func() (err error) {
			self.lock.Unlock()
			defer self.lock.Lock()
			if err = next(self); err != nil {
				return
			}
			return
		}(); err != nil {
			return
		}
	}
	self.afterTransaction = nil
	return
}

/*
View opens a read only transaction.
*/
func (self *DB) View(f func(tx *TX) error) (err error) {
	if err = self.db.View(func(boltTx *bolt.Tx) error {
		return f(&TX{
			tx: boltTx,
			db: self,
		})
	}); err != nil {
		return
	}
	if err = self.runAfterTransaction(); err != nil {
		return
	}
	return
}

/*
Update opens a read/write transaction.
*/
func (self *DB) Update(f func(tx *TX) error) (err error) {
	if err = self.db.Update(func(boltTx *bolt.Tx) error {
		return f(&TX{
			tx: boltTx,
			db: self,
		})
	}); err != nil {
		return
	}
	if err = self.runAfterTransaction(); err != nil {
		return
	}
	return
}

/*
Unsubscribe will remove the named subscription from this DB.
*/
func (self *DB) Unsubscribe(name string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for _, typeSubs := range self.subscriptions {
		delete(typeSubs, name)
	}
}

/*
Subscription will return a subscription with name, watching changes to objects with the same type and id as obj.
It will watch for operations matching the ops, and send the events to the subscriber.
*/
func (self *DB) Subscription(name string, obj interface{}, ops Operation, subscriber Subscriber) (result *Subscription, err error) {
	var wantedValue reflect.Value
	var wantedId reflect.Value
	if wantedValue, wantedId, err = identify(obj); err != nil {
		return
	}
	wantedType := wantedValue.Type()
	wantedBytes := make([]byte, len(wantedId.Bytes()))
	copy(wantedBytes, wantedId.Bytes())
	result = &Subscription{
		name: name,
		db:   self,
		matcher: func(tx *TX, typ reflect.Type, value reflect.Value) (result bool, err error) {
			if typ.Name() != wantedType.Name() {
				return
			}
			if bytes.Compare(value.FieldByName(idField).Bytes(), wantedBytes) != 0 {
				return
			}
			result = true
			return
		},
		subscriber: subscriber,
		ops:        ops,
		typ:        wantedType,
	}
	return
}

/*
EmitUpdate will artificially emit an update on obj, that will cause all subscriptions for update operations on matching objects get an update event.
*/
func (self *DB) EmitUpdate(obj interface{}) (err error) {
	value := reflect.ValueOf(obj).Elem()
	return self.emit(reflect.TypeOf(value.Interface()), &value, &value)
}

func (self *DB) emit(typ reflect.Type, oldValue, newValue *reflect.Value) (err error) {
	if oldValue != nil && newValue != nil {
		if chain := newValue.Addr().MethodByName("Updated"); chain.IsValid() {
			if err = callErr(chain, []reflect.Value{reflect.ValueOf(self), oldValue.Addr()}); err != nil {
				return
			}
		}
	} else if newValue != nil {
		if chain := newValue.Addr().MethodByName("Created"); chain.IsValid() {
			if err = callErr(chain, []reflect.Value{reflect.ValueOf(self)}); err != nil {
				return
			}
		}
	} else if oldValue != nil {
		if chain := oldValue.Addr().MethodByName("Deleted"); chain.IsValid() {
			if err = callErr(chain, []reflect.Value{reflect.ValueOf(self)}); err != nil {
				return
			}
		}
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	for _, subscription := range self.subscriptions[typ.Name()] {
		go subscription.handle(typ, oldValue, newValue)
	}
	return
}

/*
Query returns a new query for DB.
*/
func (self *DB) Query() *Query {
	return &Query{
		db: self,
		run: func(f func(*TX) error) error {
			return self.View(f)
		},
	}
}
