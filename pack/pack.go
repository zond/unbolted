package pack

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zond/unbolted"
	"github.com/zond/wsubs"
)

/*
Subscription encapsulates a subscription by a WebSocket on an object or a query.
*/
type Subscription struct {
	pack  *Pack
	uri   string
	name  string
	Query *unbolted.Query
	/*
		Call defaults to Subscription.Send, and is used to deliver all data for this Subscription.

		Replace it if you want to filter or decorate the data before sending it with Subscription.Send.
	*/
	Call func(o interface{}, op string) error
	/*
	  OnUnsubscribe will be called if this Subscription gets automatically unsubscribed due to panic or error.
	*/
	UnsubscribeListener func(self *Subscription, reason interface{})
	/*
	  Logger will be called whenever data is sent over the socket.
	*/
	Logger func(o interface{}, op string, dur time.Duration)
}

/*
Name returns the name of the Subscription.
*/
func (self *Subscription) Name() string {
	return self.name
}

/*
URI returns the URI of the subscription.
*/
func (self *Subscription) URI() string {
	return self.uri
}

/*
Send will send a message through the WebSocket of this Subscription.

Message.Type will be op, Message.Object.URI will be the uri of this subscription and Message.Object.Data will be the JSON representation of i.
*/
func (self *Subscription) Send(i interface{}, op string) (err error) {
	return self.pack.ws.WriteJSON(wsubs.Message{
		Type: op,
		Object: &wsubs.Object{
			Data: i,
			URI:  self.uri,
		},
	})
}

/*
DB returns the DB of the Pack that created this Subscription.
*/
func (self *Subscription) DB() *unbolted.DB {
	return self.pack.db
}

/*
Subscribe will start this Subscription.

If the Subscription has a Query, the results of the Query will be sent through the WebSocket, and then a subscription for this query will start that continues
sending updates on the results of the query through the WebSocket.

If the Subscription doesn't have a Query, the object will be loaded from the database and sent through the websocket, and then a subscription for that object will start that
continues sending updates on the object through the WebSocket.
*/
func (self *Subscription) Subscribe(object interface{}) (err error) {
	start := time.Now()
	var sub *unbolted.Subscription
	if self.Query == nil {
		if sub, err = self.pack.db.Subscription(self.name, object, unbolted.AllOps, func(i interface{}, op unbolted.Operation) error {
			return self.Call(i, op.String())
		}); err != nil {
			return
		}
	} else {
		if sub, err = self.Query.Subscription(self.name, object, unbolted.AllOps, func(i interface{}, op unbolted.Operation) error {
			slice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(object)), 1, 1)
			slice.Index(0).Set(reflect.ValueOf(i))
			return self.Call(slice.Interface(), op.String())
		}); err != nil {
			return
		}
	}
	if self.UnsubscribeListener != nil {
		sub.UnsubscribeListener = func(name string, reason interface{}) {
			self.UnsubscribeListener(self, reason)
		}
	}
	if self.Logger != nil {
		sub.Logger = func(i interface{}, op unbolted.Operation, dur time.Duration) {
			self.Logger(i, op.String(), dur)
		}
	}
	sub.Subscribe()
	self.pack.lock.Lock()
	defer self.pack.lock.Unlock()
	self.pack.subs[self.name] = self
	if self.Query == nil {
		if err = self.pack.db.View(func(tx *unbolted.TX) (err error) {
			if err = tx.Get(object); err != nil {
				if err == unbolted.ErrNotFound {
					err = nil
				} else {
					return
				}
			}
			if self.Logger != nil {
				defer func() {
					self.Logger(object, wsubs.FetchType, time.Now().Sub(start))
				}()
			}
			return self.Call(object, gosubs.FetchType)
		}); err != nil {
			return
		}
	} else {
		slice := reflect.New(reflect.SliceOf(reflect.TypeOf(object))).Interface()
		if err = self.Query.All(slice); err != nil {
			return
		}
		iface := reflect.ValueOf(slice).Elem().Interface()
		if self.Logger != nil {
			defer func() {
				self.Logger(iface, gosubs.FetchType, time.Now().Sub(start))
			}()
		}
		return self.Call(iface, gosubs.FetchType)
	}
	return
}

/*
Pack encapsulates a set of Subscriptions from a single WebSocket connected to a single DB.

Use it to unsubscribe all Subscriptions when the WebSocket disconnects.
*/
type Pack struct {
	db                  *unbolted.DB
	ws                  *websocket.Conn
	lock                *sync.Mutex
	subs                map[string]*Subscription
	unsubscribeListener func(sub *Subscription, reason interface{})
	logger              func(uri string, i interface{}, op string, dur time.Duration)
}

/*
New will return a new Pack for db and ws.
*/
func NewPack(db *unbolted.DB, ws *websocket.Conn) *Pack {
	return &Pack{
		lock: new(sync.Mutex),
		subs: make(map[string]*Subscription),
		ws:   ws,
		db:   db,
	}
}

func (self *Pack) OnUnsubscribe(f func(sub *Subscription, reason interface{})) *Pack {
	self.unsubscribeListener = f
	return self
}

func (self *Pack) Logger(f func(name string, i interface{}, op string, dur time.Duration)) *Pack {
	self.logger = f
	return self
}

func (self *Pack) generateName(uri string) string {
	return fmt.Sprintf("%v/%v", self.ws.RemoteAddr(), uri)
}

func (self *Pack) unsubscribeName(name string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	if _, found := self.subs[name]; found {
		self.db.Unsubscribe(name)
		delete(self.subs, name)
	}
}

/*
Unsubscribe will unsubscribe the Subscription for uri.
*/
func (self *Pack) Unsubscribe(uri string) {
	self.unsubscribeName(self.generateName(uri))
}

/*
UnsubscribeAll will unsubscribe all Subscriptions of this Pack.
*/
func (self *Pack) UnsubscribeAll() {
	self.lock.Lock()
	defer self.lock.Unlock()
	for name, _ := range self.subs {
		self.db.Unsubscribe(name)
	}
	self.subs = make(map[string]*Subscription)
}

/*
New will return a new Subscription using the WebSocket and database of this Pack, bound to uri.

The new Subscription will have Call set to its Send func.
*/
func (self *Pack) New(uri string) (result *Subscription) {
	result = &Subscription{
		pack:                self,
		uri:                 uri,
		name:                self.generateName(uri),
		UnsubscribeListener: self.unsubscribeListener,
		Logger: func(i interface{}, op string, dur time.Duration) {
			self.logger(uri, i, op, dur)
		},
	}
	result.Call = result.Send
	return
}
