package unbolted

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

type testStruct struct {
	Id        []byte
	Name      string `unbolted:"index"`
	Age       int    `unbolted:"index"`
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestCRUD(t *testing.T) {
	d, err := NewDB("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
	if err := d.Clear(); err != nil {
		t.Fatalf(err.Error())
	}
	mock := &testStruct{Id: []byte("hepp")}
	if err := d.Del(mock); err != ErrNotFound {
		t.Fatalf("Wanted an ErrNotFound")
	}
	hehu := testStruct{
		Name: "hehu",
		Age:  12,
	}
	if err := d.Set(&hehu); err != nil {
		t.Fatalf(err.Error())
	}
	if hehu.Id == nil {
		t.Fatalf("Did not create id")
	}
	hehu2 := testStruct{Id: hehu.Id}
	if err := d.Get(&hehu2); err != nil {
		t.Fatalf(err.Error())
	}
	if !reflect.DeepEqual(hehu, hehu2) {
		t.Fatalf("Did not get the same data, wanted %+v but got %+v", hehu, hehu2)
	}
	hehu2.Age = 13
	if err := d.Set(&hehu2); err != nil {
		t.Fatalf(err.Error())
	}
	if bytes.Compare(hehu2.Id, hehu.Id) != 0 {
		t.Fatalf("Changed id")
	}
	hehu3 := testStruct{Id: hehu.Id}
	if err := d.Get(&hehu3); err != nil {
		t.Fatalf(err.Error())
	}
	if !reflect.DeepEqual(hehu2, hehu3) {
		t.Fatalf("Did not get the same data")
	}
	if bytes.Compare(hehu3.Id, hehu.Id) != 0 {
		t.Fatalf("Changed id")
	}
	if err := d.Del(&hehu); err != nil {
		t.Fatalf(err.Error())
	}
	hehu4 := testStruct{Id: hehu.Id}
	if err := d.Get(&hehu4); err != ErrNotFound {
		t.Fatalf(err.Error())
	}
}

func isAlmost(t1, t2 time.Time) bool {
	if diff := t1.Sub(t2); diff > time.Millisecond || diff < -time.Millisecond {
		return false
	}
	return true
}

func TestCreatedAt(t *testing.T) {
	d, err := NewDB("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
	d.Clear()
	ts := &testStruct{}
	if err := d.Set(ts); err != nil {
		t.Fatalf(err.Error())
	}
	if ts.CreatedAt.IsZero() {
		t.Fatalf("Wanted non nil")
	}
	if ts.UpdatedAt.IsZero() {
		t.Fatalf("Wanted non nil")
	}
	if !isAlmost(ts.UpdatedAt, ts.CreatedAt) {
		t.Fatalf("Wanted equal")
	}
	oldUpd := ts.UpdatedAt
	oldCre := ts.CreatedAt
	ts.Name = "hehu"
	if err := d.Set(ts); err != nil {
		t.Fatalf(err.Error())
	}
	if oldUpd.Equal(ts.UpdatedAt) {
		t.Fatalf("Wanted non equal")
	}
	if !oldCre.Equal(ts.CreatedAt) {
		t.Fatalf("Wanted equal")
	}
}

type ExampleStruct struct {
	Id             []byte
	SomeField      string
	SomeOtherField int
}

func ExampleCRUD() {
	// open the databse file "example" and panic if fail
	d := MustDB("example")
	// start a write transaction
	if err := d.Update(func(tx *TX) (err error) {
		// clear the database from previous example runs
		if err = tx.Clear(); err != nil {
			return
		}
		// create an example value without id
		exampleValue := &ExampleStruct{
			SomeField: "some value",
		}
		// put it in the database. this will give it an id
		if err = tx.Set(exampleValue); err != nil {
			return
		}
		// create an empty value, but with an id
		loadedValue := &ExampleStruct{
			Id: exampleValue.Id,
		}
		// load it from the database. this will fill out the values using whatever is in the database with this id
		if err = tx.Get(loadedValue); err != nil {
			return
		}
		fmt.Println(loadedValue.SomeField)
		return
	}); err != nil {
		panic(err)
	}
	// Output:
	// some value
}

func TestQuery(t *testing.T) {
	d, err := NewDB("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
	if err := d.Clear(); err != nil {
		t.Fatalf(err.Error())
	}
	hehu := testStruct{
		Name: "hehu",
		Age:  12,
	}
	if err := d.Set(&hehu); err != nil {
		t.Fatalf(err.Error())
	}
	var res []testStruct
	if err := d.Query().All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	wanted := []testStruct{
		hehu,
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Fatalf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Where(Equals{"Name", "hehu"}).All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Fatalf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Where(Equals{"Name", "blapp"}).All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if len(res) != 0 {
		t.Fatalf("Wanted [] but got %v", res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "hehu"}, Equals{"Age", 12}}).All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Fatalf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "blapp"}, Equals{"Age", 11}}).All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if len(res) != 0 {
		t.Fatalf("Wanted [] but got %v", res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 12}, Equals{"Age", 11}}}).All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Fatalf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "blapp"}, Or{Equals{"Age", 11}, Equals{"Age", 13}}}).All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if len(res) != 0 {
		t.Fatalf("Wanted [] but got %v", res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 12}, Equals{"Age", 11}}}).Except(Equals{"Name", "blapp"}).All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Fatalf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "blapp"}, Or{Equals{"Age", 11}, Equals{"Age", 13}}}).Except(Equals{"Name", "hehu"}).All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if len(res) != 0 {
		t.Fatalf("Wanted [] but got %v", res)
	}
	var res2 testStruct
	if found, err := d.Query().Where(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 11}, Equals{"Age", 12}}}).Except(Equals{"Name", "blapp"}).First(&res2); err != nil || !found {
		t.Fatalf("%v, %v", found, err)
	}
	if !reflect.DeepEqual(hehu, res2) {
		t.Fatalf("Wanted %v but got %v", hehu, res2)
	}
}

type subTester struct {
	d       *DB
	t       *testing.T
	targ    interface{}
	sub     *Subscription
	removed []*testStruct
	created []*testStruct
	updated []*testStruct
	lock    sync.RWMutex
	event   chan struct{}
}

func newSubTester(t *testing.T, d *DB) (result *subTester) {
	result = &subTester{
		t:     t,
		d:     d,
		event: make(chan struct{}),
	}
	result.event = make(chan struct{})
	return result
}

func (self *subTester) subscribe() *subTester {
	self.sub.Subscribe()
	return self
}

func (self *subTester) target(targ interface{}) *subTester {
	var err error
	if self.sub, err = self.d.Subscription("subtest1", targ, AllOps, func(obj interface{}, op Operation) error {
		self.lock.Lock()
		defer self.lock.Unlock()
		switch op {
		case Delete:
			self.removed = append(self.removed, obj.(*testStruct))
		case Create:
			self.created = append(self.created, obj.(*testStruct))
		case Update:
			self.updated = append(self.updated, obj.(*testStruct))
		}
		self.event <- struct{}{}
		return nil
	}); err != nil {
		self.t.Fatalf(err.Error())
	}
	return self
}

func (self *subTester) assert(rem, cre, upd []*testStruct) {
	select {
	case <-self.event:
	case <-time.After(time.Millisecond * 50):
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	where := strings.Split(string(debug.Stack()), "\n")[2]
	if !reflect.DeepEqual(rem, self.removed) {
		self.t.Fatalf("%v: Wanted %v to have been deleted, but got %v", where, rem, self.removed)
	}
	if !reflect.DeepEqual(cre, self.created) {
		self.t.Fatalf("%v: Wanted %v to have been created, but got %v", where, cre, self.created)
	}
	if !reflect.DeepEqual(upd, self.updated) {
		self.t.Fatalf("%v: Wanted %v to have been updated, but got %v", where, upd, self.updated)
	}
}

func TestIdSubscribe(t *testing.T) {
	d, err := NewDB("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
	if err := d.Clear(); err != nil {
		t.Fatalf(err.Error())
	}
	hehu := testStruct{
		Name: "hehu",
		Age:  12,
	}
	if err := d.Set(&hehu); err != nil {
		t.Fatalf(err.Error())
	}
	testSub := newSubTester(t, d).target(&hehu).subscribe()
	if err := d.Del(&hehu); err != nil {
		t.Fatalf(err.Error())
	}
	testSub.assert([]*testStruct{&hehu}, nil, nil)
	hehu2 := hehu
	hehu2.Name = "blepp"
	if err := d.Set(&hehu2); err != nil {
		t.Fatalf(err.Error())
	}
	testSub.assert([]*testStruct{&hehu}, []*testStruct{&hehu2}, nil)
	hehu3 := hehu2
	hehu3.Name = "jaja"
	if err := d.Set(&hehu3); err != nil {
		t.Fatalf(err.Error())
	}
	testSub.assert([]*testStruct{&hehu}, []*testStruct{&hehu2}, []*testStruct{&hehu3})
	hehu4 := testStruct{
		Name: "knasen",
	}
	if err := d.Set(&hehu4); err != nil {
		t.Fatalf(err.Error())
	}
	time.Sleep(time.Millisecond * 100)
	testSub.assert([]*testStruct{&hehu}, []*testStruct{&hehu2}, []*testStruct{&hehu3})
}

func (self *subTester) query(q *Query, targ interface{}) *subTester {
	var err error
	if self.sub, err = q.Subscription("subtest1", targ, AllOps, func(obj interface{}, op Operation) error {
		self.lock.Lock()
		defer self.lock.Unlock()
		switch op {
		case Delete:
			self.removed = append(self.removed, obj.(*testStruct))
		case Create:
			self.created = append(self.created, obj.(*testStruct))
		case Update:
			self.updated = append(self.updated, obj.(*testStruct))
		}
		self.event <- struct{}{}
		return nil
	}); err != nil {
		self.t.Fatalf(err.Error())
	}
	return self
}

func TestQuerySubscribe(t *testing.T) {
	d, err := NewDB("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if err := d.Clear(); err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
	hehu := testStruct{}
	testSub := newSubTester(t, d).query(d.Query().Where(Equals{"Name", "qname"}), &testStruct{}).subscribe()
	hehu.Name = "qname"
	if err := d.Set(&hehu); err != nil {
		t.Errorf(err.Error())
	}
	testSub.assert(nil, []*testStruct{&hehu}, nil)
	hehu2 := hehu
	hehu2.Age = 31
	if err := d.Set(&hehu2); err != nil {
		t.Errorf(err.Error())
	}
	testSub.assert(nil, []*testStruct{&hehu}, []*testStruct{&hehu2})
	if err := d.Del(&hehu2); err != nil {
		t.Errorf(err.Error())
	}
	testSub.assert([]*testStruct{&hehu2}, []*testStruct{&hehu}, []*testStruct{&hehu2})
	hehu3 := hehu2
	hehu3.Name = "othername"
	if err := d.Set(&hehu3); err != nil {
		t.Errorf(err.Error())
	}
	testSub.assert([]*testStruct{&hehu2}, []*testStruct{&hehu}, []*testStruct{&hehu2})
}

type phase struct {
	Id   []byte
	Game []byte
}

func (self *phase) Updated(d *DB, old *phase) (err error) {
	g := game{Id: self.Game}
	if err = d.Get(&g); err != nil {
		return
	}
	return d.EmitUpdate(&g)
}

type game struct {
	Id []byte
}

func (self *game) Updated(d *DB, old *game) (err error) {
	var members []member
	if err = d.Query().Where(Equals{"Game", self.Id}).All(&members); err != nil {
		return
	}
	for _, member := range members {
		cpy := member
		if err = d.EmitUpdate(&cpy); err != nil {
			return
		}
	}
	return
}

type member struct {
	Id   []byte
	User []byte
	Game []byte `unbolted:"index"`
}

func (self *member) Updated(d *DB, old *member) {
	close(globalTestLock)
}

type user struct {
	Id []byte
}

var globalTestLock chan bool

func TestChains(t *testing.T) {
	d, err := NewDB("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if err := d.Clear(); err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
	u := user{}
	if err := d.Set(&u); err != nil {
		t.Fatalf(err.Error())
	}
	g := game{}
	if err := d.Set(&g); err != nil {
		t.Fatalf(err.Error())
	}
	p := phase{Game: g.Id}
	if err := d.Set(&p); err != nil {
		t.Fatalf(err.Error())
	}
	m := member{Game: g.Id, User: u.Id}
	if err := d.Set(&m); err != nil {
		t.Fatalf(err.Error())
	}
	globalTestLock = make(chan bool)
	if err := d.Set(&p); err != nil {
		t.Fatalf(err.Error())
	}
	select {
	case <-globalTestLock:
	case <-time.After(time.Second):
		t.Fatalf("Didn't get the user update!")
	}
}
