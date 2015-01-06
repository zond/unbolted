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
	if err := d.Update(func(tx *TX) (err error) {
		tx.Clear()
		hehu := testStruct{
			Name: "hehu",
			Age:  12,
		}
		if err := tx.Set(&hehu); err != nil {
			t.Fatalf(err.Error())
		}
		var res []testStruct
		if err := tx.Query().All(&res); err != nil {
			t.Fatalf(err.Error())
		}
		wanted := []testStruct{
			hehu,
		}
		if !reflect.DeepEqual(res, wanted) {
			t.Fatalf("Wanted %v but got %v", wanted, res)
		}
		res = nil
		if err := tx.Query().Where(Equals{"Name", "hehu"}).All(&res); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(res, wanted) {
			t.Fatalf("Wanted %v but got %v", wanted, res)
		}
		res = nil
		if err := tx.Query().Where(Equals{"Name", "blapp"}).All(&res); err != nil {
			t.Fatalf(err.Error())
		}
		if len(res) != 0 {
			t.Fatalf("Wanted [] but got %v", res)
		}
		res = nil
		if err := tx.Query().Where(And{Equals{"Name", "hehu"}, Equals{"Age", 12}}).All(&res); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(res, wanted) {
			t.Fatalf("Wanted %v but got %v", wanted, res)
		}
		res = nil
		if err := tx.Query().Where(And{Equals{"Name", "blapp"}, Equals{"Age", 11}}).All(&res); err != nil {
			t.Fatalf(err.Error())
		}
		if len(res) != 0 {
			t.Fatalf("Wanted [] but got %v", res)
		}
		res = nil
		if err := tx.Query().Where(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 12}, Equals{"Age", 11}}}).All(&res); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(res, wanted) {
			t.Fatalf("Wanted %v but got %v", wanted, res)
		}
		res = nil
		if err := tx.Query().Where(And{Equals{"Name", "blapp"}, Or{Equals{"Age", 11}, Equals{"Age", 13}}}).All(&res); err != nil {
			t.Fatalf(err.Error())
		}
		if len(res) != 0 {
			t.Fatalf("Wanted [] but got %v", res)
		}
		res = nil
		if err := tx.Query().Where(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 12}, Equals{"Age", 11}}}).Except(Equals{"Name", "blapp"}).All(&res); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(res, wanted) {
			t.Fatalf("Wanted %v but got %v", wanted, res)
		}
		res = nil
		if err := tx.Query().Where(And{Equals{"Name", "blapp"}, Or{Equals{"Age", 11}, Equals{"Age", 13}}}).Except(Equals{"Name", "hehu"}).All(&res); err != nil {
			t.Fatalf(err.Error())
		}
		if len(res) != 0 {
			t.Fatalf("Wanted [] but got %v", res)
		}
		var res2 testStruct
		if found, err := tx.Query().Where(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 11}, Equals{"Age", 12}}}).Except(Equals{"Name", "blapp"}).First(&res2); err != nil || !found {
			t.Fatalf("%v, %v", found, err)
		}
		if !reflect.DeepEqual(hehu, res2) {
			t.Fatalf("Wanted %v but got %v", hehu, res2)
		}
		return
	}); err != nil {
		t.Fatalf(err.Error())
	}
}

type subTester struct {
	d       *DB
	ts      *testStruct
	t       *testing.T
	sub     *Subscription
	removed []*testStruct
	created []*testStruct
	updated []*testStruct
	lock    sync.RWMutex
	event   chan struct{}
}

func startSubTester(t *testing.T, d *DB, ts *testStruct) (result *subTester) {
	result = &subTester{
		t:     t,
		d:     d,
		ts:    ts,
		event: make(chan struct{}),
	}
	result.start()
	return result
}

func (self *subTester) start() {
	self.event = make(chan struct{})
	var err error
	if self.sub, err = self.d.Subscription("subtest1", self.ts, AllOps, func(obj interface{}, op Operation) error {
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
	self.sub.Subscribe()
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
	testSub := startSubTester(t, d, &hehu)
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
