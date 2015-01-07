package pack

import (
	"github.com/zond/unbolted"
	"github.com/zond/wsubs/gosubs"
)

type SubContext interface {
	gosubs.Context
	Pack() *Pack
	DB() *unbolted.DB
}

type Context interface {
	SubContext
	AfterTransaction(func(Context) error) error
	View(func(TXContext) error) error
	Update(func(TXContext) error) error
}

type TXContext interface {
	Context
	TX() *unbolted.TX
}

func NewContext(c gosubs.Context, pack *Pack, router *Router) Context {
	return &defaultContext{
		Context: c,
		pack:    pack,
		router:  router,
		db:      router.DB,
	}
}

type defaultContext struct {
	gosubs.Context
	pack   *Pack
	router *Router
	db     *unbolted.DB
}

func (self *defaultContext) AfterTransaction(f func(Context) error) (err error) {
	return self.db.AfterTransaction(func(d *unbolted.DB) (err error) {
		return f(self)
	})
}

func (self *defaultContext) View(f func(c TXContext) error) error {
	return self.db.View(func(tx *unbolted.TX) error {
		return f(&defaultTXContext{
			defaultContext: self,
			tx:             tx,
		})
	})
}

func (self *defaultContext) Update(f func(c TXContext) error) error {
	return self.db.Update(func(tx *unbolted.TX) error {
		return f(&defaultTXContext{
			defaultContext: self,
			tx:             tx,
		})
	})
}

func (self *defaultContext) DB() *unbolted.DB {
	return self.db
}

func (self *defaultContext) Pack() *Pack {
	return self.pack
}

func (self *defaultContext) Router() *Router {
	return self.router
}

type defaultTXContext struct {
	*defaultContext
	tx *unbolted.TX
}

func (self *defaultTXContext) TX() *unbolted.TX {
	return self.tx
}
