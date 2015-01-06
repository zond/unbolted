package unbolted

import (
	"fmt"
	"sync"

	"github.com/boltdb/bolt"
)

type DB struct {
	db               *bolt.DB
	lock             sync.RWMutex
	subscriptions    map[string]map[string]*Subscription
	afterTransaction []func(*DB) error
}

func (self *DB) String() string {
	return fmt.Sprintf("&bobject.DB@%p{db:%v,subscriptions:%v}", self, self.db, self.subscriptions)
}

func MustDB(path string) (result *DB) {
	result, err := NewDB(path)
	if err != nil {
		panic(err)
	}
	return
}

func NewDB(path string) (result *DB, err error) {
	result = &DB{
		subscriptions: make(map[string]map[string]*Subscription),
	}
	if result.db, err = bolt.Open(path, 0600, nil); err != nil {
		return
	}
	return
}

func (self *DB) Close() (err error) {
	return self.db.Close()
}

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

func (self *DB) Set(obj interface{}) (err error) {
	return self.Update(func(tx *TX) error { return tx.Set(obj) })
}

func (self *DB) Get(obj interface{}) error {
	return self.View(func(tx *TX) error { return tx.Get(obj) })
}

func (self *DB) Del(obj interface{}) (err error) {
	return self.Update(func(tx *TX) error { return tx.Del(obj) })
}

func (self *DB) Clear() (err error) {
	return self.Update(func(tx *TX) error { return tx.Clear() })
}
