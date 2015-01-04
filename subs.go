package unbolted

import (
	"bytes"
	"fmt"
	"reflect"
	"time"
)

type Operation int

func (self Operation) String() string {
	switch self {
	case Create:
		return "Create"
	case Update:
		return "Update"
	case Delete:
		return "Delete"
	}
	panic(fmt.Errorf("Unknown Operation: %v", self))
}

const (
	Create Operation = 1 << iota
	Update
	Delete
)

// AllOps is the binary OR of all the database operations you can subscribe to.
var AllOps = Create | Update | Delete

/*
Subscribers get updates when objects are updated.
If the Subscriber returns an error or panics, it will be unsubscribed.
*/
type Subscriber func(obj interface{}, op Operation) error

/*
UnsubscribeListener is used to notify a user of a Subscriber that the Subscriber
has been unsubscribed.
*/
type UnsubscribeListener func(name string, reason interface{})

/*
Logger is used to log and/or measure what the subscribers do.
*/
type Logger func(i interface{}, op Operation, dur time.Duration)

type matcher func(typ reflect.Type, value reflect.Value) (result bool, err error)

type Subscription struct {
	db                  *DB
	name                string
	matcher             matcher
	subscriber          Subscriber
	UnsubscribeListener UnsubscribeListener
	Logger              Logger
	ops                 Operation
	typ                 reflect.Type
}

/*
Subscribe will start the subscription.
*/
func (self *Subscription) Subscribe() {
	self.db.lock.Lock()
	defer self.db.lock.Unlock()
	typeSubs, found := self.db.subscriptions[self.typ.Name()]
	if !found {
		typeSubs = make(map[string]*Subscription)
		self.db.subscriptions[self.typ.Name()] = typeSubs
	}
	typeSubs[self.name] = self
	return
}

/*
Unsubscribe will unsubscribe this Subscription with the given reason.
*/
func (self *Subscription) Unsubscribe(reason interface{}) {
	self.db.Unsubscribe(self.name)
	if self.UnsubscribeListener != nil {
		self.UnsubscribeListener(self.name, reason)
	}
}

func (self *Subscription) call(obj interface{}, op Operation, start time.Time) {
	if err := self.subscriber(obj, op); err != nil {
		self.Unsubscribe(err)
	} else if self.Logger != nil {
		self.Logger(obj, op, time.Now().Sub(start))
	}
}

func (self *Subscription) handle(typ reflect.Type, oldValue, newValue *reflect.Value) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			self.Unsubscribe(e)
			panic(e)
		}
	}()
	var err error
	oldMatch := false
	newMatch := false
	if oldValue != nil {
		if oldMatch, err = self.matcher(typ, *oldValue); err != nil {
			self.Unsubscribe(err)
			return
		}
	}
	if newValue != nil {
		if newMatch, err = self.matcher(typ, *newValue); err != nil {
			self.Unsubscribe(err)
			return
		}
	}
	if oldMatch && newMatch && self.ops&Update == Update {
		cpy := reflect.New(typ)
		cpy.Elem().Set(*newValue)
		self.call(cpy.Interface(), Update, start)
	} else if oldMatch && self.ops&Delete == Delete {
		cpy := reflect.New(typ)
		cpy.Elem().Set(*oldValue)
		self.call(cpy.Interface(), Delete, start)
	} else if newMatch && self.ops&Create == Create {
		cpy := reflect.New(typ)
		cpy.Elem().Set(*newValue)
		self.call(cpy.Interface(), Create, start)
	}
}

/*
Unsubscribe will remove a Subscription.
*/
func (self *DB) Unsubscribe(name string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for _, typeSubs := range self.subscriptions {
		delete(typeSubs, name)
	}
}

/*
Subscription will return a Subscription to all updates of a given object in the database.

name is used to separate different Subscriptions, and to unsubscribe.

ops is the binary OR of the operations this Subscription should follow.

subscriber will be called on all updates of objects with the same id.
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
		matcher: func(typ reflect.Type, value reflect.Value) (result bool, err error) {
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
EmitUpdate will trigger an Update event on obj.

Useful when chaining events, such as when an update of an inner objects
should cause an updated of an outer object.
*/
func (self *DB) EmitUpdate(obj interface{}) (err error) {
	value := reflect.ValueOf(obj).Elem()
	return self.emit(reflect.TypeOf(value.Interface()), &value, &value)
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
