package state

import (
	"context"
	"testing"
)


type foo struct {
	Worker
	aaa string
	bbb int
}

func newFoo () foo {
	f := foo{}
	return f
	
}

func (f foo)Name() string {
	return "foo"
}

func fooDoOne(ctx context.Context, g group[foo], fd foo) (State[foo], foo) {
	fd.aaa = "one"
	fd.bbb++
	if fd.bbb > 10 {
		return g.GetState("done"), fd
	}
	return g.GetState("foo-do-one"), fd
	
}

func TestOne(t *testing.T) {
	ctx := context.Background()
	m := NewMog()
	m.RegOp(newFoo)
	g := NewGroup[foo]()
	f := m.Gen("foo").(foo)
	g.RegisterState("foo-do-one", fooDoOne)
	fd, _ := g.runS(ctx, f, g.GetState("foo-do-one"))
	
	t.Logf("fd: %+v", fd)
	
}

