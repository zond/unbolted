package unbolted

import (
	"bytes"
	"fmt"
	"reflect"
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
	if err := d.Update(func(tx *TX) (err error) {
		if err := tx.Clear(); err != nil {
			t.Fatalf(err.Error())
		}
		mock := &testStruct{Id: []byte("hepp")}
		if err := tx.Del(mock); err != ErrNotFound {
			t.Fatalf("Wanted an ErrNotFound")
		}
		hehu := testStruct{
			Name: "hehu",
			Age:  12,
		}
		if err := tx.Set(&hehu); err != nil {
			t.Fatalf(err.Error())
		}
		if hehu.Id == nil {
			t.Fatalf("Did not create id")
		}
		hehu2 := testStruct{Id: hehu.Id}
		if err := tx.Get(&hehu2); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(hehu, hehu2) {
			t.Fatalf("Did not get the same data, wanted %+v but got %+v", hehu, hehu2)
		}
		hehu2.Age = 13
		if err := tx.Set(&hehu2); err != nil {
			t.Fatalf(err.Error())
		}
		if bytes.Compare(hehu2.Id, hehu.Id) != 0 {
			t.Fatalf("Changed id")
		}
		hehu3 := testStruct{Id: hehu.Id}
		if err := tx.Get(&hehu3); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(hehu2, hehu3) {
			t.Fatalf("Did not get the same data")
		}
		if bytes.Compare(hehu3.Id, hehu.Id) != 0 {
			t.Fatalf("Changed id")
		}
		if err := tx.Del(&hehu); err != nil {
			t.Fatalf(err.Error())
		}
		hehu4 := testStruct{Id: hehu.Id}
		if err := tx.Get(&hehu4); err != ErrNotFound {
			t.Fatalf(err.Error())
		}
		return
	}); err != nil {
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
	if err := d.Update(func(tx *TX) (err error) {
		tx.Clear()
		ts := &testStruct{}
		if err := tx.Set(ts); err != nil {
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
		if err := tx.Set(ts); err != nil {
			t.Fatalf(err.Error())
		}
		if oldUpd.Equal(ts.UpdatedAt) {
			t.Fatalf("Wanted non equal")
		}
		if !oldCre.Equal(ts.CreatedAt) {
			t.Fatalf("Wanted equal")
		}
		return
	}); err != nil {
		t.Fatalf(err.Error())
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
