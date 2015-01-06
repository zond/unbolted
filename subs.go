package unbolted

import (
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

type matcher func(tx *TX, typ reflect.Type, value reflect.Value) (result bool, err error)

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
	if err = self.db.View(func(tx *TX) (err error) {
		if oldValue != nil {
			if oldMatch, err = self.matcher(tx, typ, *oldValue); err != nil {
				return
			}
		}
		if newValue != nil {
			if newMatch, err = self.matcher(tx, typ, *newValue); err != nil {
				return
			}
		}
		return
	}); err != nil {
		self.Unsubscribe(err)
		return
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
