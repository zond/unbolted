package unbolted

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

type benchStruct0 struct {
	Id   []byte
	Name string
}

type benchStruct1 struct {
	Id   []byte
	Name string `unbolted:"index"`
}

type benchStruct10 struct {
	Id     []byte
	Name   string `unbolted:"index"`
	Name2  string `unbolted:"index"`
	Name3  string `unbolted:"index"`
	Name4  string `unbolted:"index"`
	Name5  string `unbolted:"index"`
	Name6  string `unbolted:"index"`
	Name7  string `unbolted:"index"`
	Name8  string `unbolted:"index"`
	Name9  string `unbolted:"index"`
	Name10 string `unbolted:"index"`
}

func withBenchDB(b *testing.B, fresh bool, f func(db *DB)) {
	if fresh {
		if err := os.Remove("bench"); err != nil {
			if os.IsNotExist(err) {
				err = nil
			} else {
				b.Fatalf(err.Error())
			}
		}
	}
	benchdb, err := NewDB("bench")
	if err != nil {
		b.Fatalf(err.Error())
	}
	defer func() {
		if err := benchdb.Close(); err != nil {
			b.Fatalf(err.Error())
		}
	}()
	f(benchdb)
}

func benchWrite(b *testing.B, objs []interface{}) {
	withBenchDB(b, true, func(benchdb *DB) {
		b.ResetTimer()
		for _, s := range objs {
			if err := benchdb.Update(func(tx *TX) error { return tx.Set(s) }); err != nil {
				b.Fatalf(err.Error())
			}
		}
	})
}

func benchGet(b *testing.B, objs []interface{}) {
	withBenchDB(b, false, func(benchdb *DB) {
		size := 0
		if err := benchdb.View(func(tx *TX) (err error) {
			size, err = tx.Count(objs[0])
			return
		}); err != nil {
			b.Fatalf(err.Error())
		}
		towrite := objs
		if len(towrite) > size {
			towrite = towrite[size:]
		}
		for _, s := range towrite {
			if err := benchdb.Update(func(tx *TX) error { return tx.Set(s) }); err != nil {
				b.Fatalf(err.Error())
			}
		}
		b.ResetTimer()
		for _, s := range objs {
			if err := benchdb.View(func(tx *TX) error { return tx.Get(s) }); err != nil {
				b.Fatalf("Unable to load %+v: %v", s, err.Error())
			}
		}
	})
}

func makeBenchStructs(b *testing.B, gen func(rand.Source) interface{}) (result []interface{}) {
	source := rand.NewSource(0)
	result = make([]interface{}, b.N)
	for index, _ := range result {
		result[index] = gen(source)
	}
	return
}

func BenchmarkGet(b *testing.B) {
	benchGet(b, makeBenchStructs(b, func(source rand.Source) interface{} {
		return &benchStruct0{
			Id:   []byte(fmt.Sprintf("%v", source.Int63())),
			Name: fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
		}
	}))
}

func BenchmarkWriteNoIndex(b *testing.B) {
	benchWrite(b, makeBenchStructs(b, func(source rand.Source) interface{} {
		return &benchStruct0{
			Id:   []byte(fmt.Sprintf("%v", source.Int63())),
			Name: fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
		}
	}))
}

func BenchmarkWriteOneIndex(b *testing.B) {
	benchWrite(b, makeBenchStructs(b, func(source rand.Source) interface{} {
		return &benchStruct1{
			Id:   []byte(fmt.Sprintf("%v", source.Int63())),
			Name: fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
		}
	}))
}

func BenchmarkWrite10Index(b *testing.B) {
	benchWrite(b, makeBenchStructs(b, func(source rand.Source) interface{} {
		return &benchStruct10{
			Id:     []byte(fmt.Sprintf("%v", source.Int63())),
			Name:   fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
			Name2:  fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
			Name3:  fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
			Name4:  fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
			Name5:  fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
			Name6:  fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
			Name7:  fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
			Name8:  fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
			Name9:  fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
			Name10: fmt.Sprintf("%v%v", source.Int63(), source.Int63()),
		}
	}))
}

func (self *DB) Index(obj interface{}) (err error) {
	return self.Update(func(tx *TX) (err error) {
		return tx.Index(obj)
	})
}

func (self *DB) Set(obj interface{}) (err error) {
	return self.Update(func(tx *TX) error { return tx.Set(obj) })
}

func (self *DB) Get(obj interface{}) error {
	return self.View(func(tx *TX) error { return tx.Get(obj) })
}

func (self *DB) Count(obj interface{}) (result int, err error) {
	err = self.View(func(tx *TX) (err error) {
		result, err = tx.Count(obj)
		return
	})
	return
}

func (self *DB) Del(obj interface{}) (err error) {
	return self.Update(func(tx *TX) error { return tx.Del(obj) })
}

func (self *DB) Clear() (err error) {
	return self.Update(func(tx *TX) error { return tx.Clear() })
}

type testStruct struct {
	Id        []byte
	Name      string `unbolted:"index"`
	Age       int    `unbolted:"index"`
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func assertSize(t *testing.T, d *DB, obj interface{}, s int) {
	count, err := d.Count(obj)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if count != s {
		t.Fatalf("wrong size, wanted %v but got %v", s, count)
	}
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
	assertSize(t, d, &testStruct{}, 0)
	mock := &testStruct{Id: []byte("hepp")}
	if err := d.Del(mock); err != ErrNotFound {
		t.Fatalf("Wanted an ErrNotFound")
	}
	assertSize(t, d, &testStruct{}, 0)
	hehu := testStruct{
		Name: "hehu",
		Age:  12,
	}
	if err := d.Set(&hehu); err != nil {
		t.Fatalf(err.Error())
	}
	assertSize(t, d, &testStruct{}, 1)
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
	if globalTestLock != nil {
		close(globalTestLock)
	}
}

type user struct {
	Id []byte
}

func TestAllQuery(t *testing.T) {
	d, err := NewDB("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
	if err := d.Clear(); err != nil {
		t.Fatalf(err.Error())
	}
	user1 := &user{}
	if err := d.Set(user1); err != nil {
		t.Fatalf(err.Error())
	}
	game := &game{}
	if err := d.Set(game); err != nil {
		t.Fatalf(err.Error())
	}
	member1 := &member{
		User: user1.Id,
		Game: game.Id,
	}
	if err := d.Set(member1); err != nil {
		t.Fatalf(err.Error())
	}
	var res []member
	if err := d.Query().All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if len(res) != 1 {
		t.Fatalf("wanted 1 members, got %#v", res)
	}
	user2 := &user{}
	if err := d.Set(user2); err != nil {
		t.Fatalf(err.Error())
	}
	member2 := &member{
		User: user2.Id,
		Game: game.Id,
	}
	if err := d.Set(member2); err != nil {
		t.Fatalf(err.Error())
	}
	res = nil
	if err := d.Query().All(&res); err != nil {
		t.Fatalf(err.Error())
	}
	if len(res) != 2 {
		t.Fatalf("wanted 2 members, got %#v of %#v, %#v", res, member1, member2)
	}
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
