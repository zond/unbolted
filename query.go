package unbolted

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/zond/setop"
)

// QFilters are used to filter queries
type QFilter interface {
	source(typ reflect.Type) (result setop.SetOpSource, err error)
	match(tx *TX, typ reflect.Type, value reflect.Value) (result bool, err error)
}

// Or is a QFilter that defineds an OR operation.
type Or []QFilter

func (self Or) source(typ reflect.Type) (result setop.SetOpSource, err error) {
	op := setop.SetOp{
		Merge: setop.First,
		Type:  setop.Union,
	}
	for _, filter := range self {
		var newSource setop.SetOpSource
		if newSource, err = filter.source(typ); err != nil {
			return
		}
		op.Sources = append(op.Sources, newSource)
	}
	result.SetOp = &op
	return
}

func (self Or) match(tx *TX, typ reflect.Type, value reflect.Value) (result bool, err error) {
	for _, filter := range self {
		if result, err = filter.match(tx, typ, value); err != nil || result {
			return
		}
	}
	return
}

// And is a QFilter that defines an AND operation.
type And []QFilter

func (self And) source(typ reflect.Type) (result setop.SetOpSource, err error) {
	op := setop.SetOp{
		Merge: setop.First,
		Type:  setop.Intersection,
	}
	for _, filter := range self {
		var newSource setop.SetOpSource
		if newSource, err = filter.source(typ); err != nil {
			return
		}
		op.Sources = append(op.Sources, newSource)
	}
	result.SetOp = &op
	return
}

func (self And) match(tx *TX, typ reflect.Type, value reflect.Value) (result bool, err error) {
	result, err = true, nil
	for _, filter := range self {
		if result, err = filter.match(tx, typ, value); err != nil || !result {
			return
		}
	}
	return
}

// Equals is a QFilter that defines an == operation.
type Equals struct {
	Field string
	Value interface{}
}

func (self Equals) source(typ reflect.Type) (result setop.SetOpSource, err error) {
	value := reflect.ValueOf(self.Value)
	var b []byte
	if b, err = indexBytes(value.Type(), value); err != nil {
		return
	}
	result = setop.SetOpSource{
		Key: joinKeys([][]byte{[]byte(secondaryIndex), []byte(typ.Name()), []byte(self.Field), b}),
	}
	return
}

func (self Equals) match(tx *TX, typ reflect.Type, value reflect.Value) (result bool, err error) {
	selfValue := reflect.ValueOf(self.Value)
	bothType := selfValue.Type()
	var selfBytes []byte
	if selfBytes, err = indexBytes(bothType, selfValue); err != nil {
		return
	}
	var otherBytes []byte
	if otherBytes, err = indexBytes(bothType, value.FieldByName(self.Field)); err != nil {
		return
	}
	result = bytes.Compare(selfBytes, otherBytes) == 0
	return
}

/*
Query is a search operation using an SQL-like syntax to fetch records from the database.

Example: db.Query().Filter(And{Equals{"Name", "John"}, Or{Equals{"Surname", "Doe"}, Equals{"Surname", "Smith"}}}).All(&result)
*/
type Query struct {
	tx           *TX
	typ          reflect.Type
	intersection QFilter
	difference   QFilter
	limit        int
}

/*
Subscription will return a subscriber for all database updates matching this query.

name is used to separate different subscriptions, and to unsubscribe.

ops is the binary OR of the operations this Subscription should follow.

subscriber will be called for all updates of the results of the query.
*/
func (self *Query) Subscription(name string, obj interface{}, ops Operation, subscriber Subscriber) (result *Subscription, err error) {
	var value reflect.Value
	if value, _, err = identify(obj); err != nil {
		return
	}
	self.typ = value.Type()
	result = &Subscription{
		db:         self.tx.db,
		name:       name,
		matcher:    self.match,
		subscriber: subscriber,
		ops:        ops,
		typ:        self.typ,
	}
	return
}

func (self *Query) match(typ reflect.Type, value reflect.Value) (result bool, err error) {
	if self.typ.Name() != typ.Name() {
		return
	}
	if self.intersection != nil {
		if result, err = self.intersection.match(self.tx, typ, value); err != nil || !result {
			return
		}
	}
	if self.difference != nil {
		if result, err = self.difference.match(self.tx, typ, value); err != nil || result {
			result = true
			return
		}
	}
	result = true
	return
}

func (self *Query) each(f func(elementPointer reflect.Value) (bool, error)) (err error) {
	op := &setop.SetOp{
		Sources: []setop.SetOpSource{
			setop.SetOpSource{
				Key: joinKeys([][]byte{[]byte(primaryKey), []byte(self.typ.Name())}),
			},
		},
		Type:  setop.Intersection,
		Merge: setop.First,
	}
	if self.intersection != nil {
		source, err := self.intersection.source(self.typ)
		if err != nil {
			return err
		}
		op.Sources = append(op.Sources, source)
	}
	if self.difference != nil {
		var source setop.SetOpSource
		if source, err = self.difference.source(self.typ); err != nil {
			return
		}
		op = &setop.SetOp{
			Sources: []setop.SetOpSource{
				setop.SetOpSource{
					SetOp: op,
				},
				source,
			},
			Type:  setop.Difference,
			Merge: setop.First,
		}
	}
	limit := self.limit
	for _, kv := range self.tx.setOp(&setop.SetExpression{
		Op: op,
	}) {
		obj := reflect.New(self.typ).Interface()
		if err = json.Unmarshal(kv.Value, obj); err != nil {
			return
		}
		cont := false
		if cont, err = f(reflect.ValueOf(obj)); err != nil {
			return
		}
		if cont {
			break
		}
		if limit == 1 {
			break
		} else if limit > 1 {
			limit--
		}
	}
	return
}

// Except will add a filter excluding matching items from the results of this query.
func (self *Query) Except(f QFilter) *Query {
	self.difference = f
	return self
}

// Limit will limit the number of matches returned
func (self *Query) Limit(l int) *Query {
	self.limit = l
	return self
}

// Where will add a filter limiting the results of this query to matching items.
func (self *Query) Where(f QFilter) *Query {
	self.intersection = f
	return self
}

// First will load the first match of this query into result.
func (self *Query) First(result interface{}) (found bool, err error) {
	var value reflect.Value
	if value, _, err = identify(result); err != nil {
		return
	}
	self.typ = value.Type()
	if err = self.each(func(elementPointer reflect.Value) (cont bool, err error) {
		value.Set(elementPointer.Elem())
		found = true
		return
	}); err != nil {
		return
	}
	return
}

// All will load all results of this quer into result.
func (self *Query) All(result interface{}) (err error) {
	slicePtrValue := reflect.ValueOf(result)
	if slicePtrValue.Kind() != reflect.Ptr {
		err = fmt.Errorf("%v is not a pointer", result)
		return
	}
	sliceValue := slicePtrValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		err = fmt.Errorf("%v is not a pointer to a slice", result)
		return
	}
	sliceType := sliceValue.Type()
	sliceElemType := sliceType.Elem()
	pointerSlice := false
	if sliceElemType.Kind() == reflect.Ptr {
		pointerSlice = true
		sliceElemType = sliceElemType.Elem()
	}
	if sliceElemType.Kind() != reflect.Struct {
		err = fmt.Errorf("%v is not pointer to a slice of structs or structpointers", result)
		return
	}
	self.typ = sliceElemType
	err = self.each(func(elementPointer reflect.Value) (cont bool, err error) {
		if pointerSlice {
			sliceValue.Set(reflect.Append(sliceValue, elementPointer))
		} else {
			sliceValue.Set(reflect.Append(sliceValue, elementPointer.Elem()))
		}
		cont = true
		return
	})
	return
}
