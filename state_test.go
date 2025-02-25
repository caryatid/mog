package state

import (
	"context"
	"golang.org/x/sync/errgroup"
	"testing"
)

type foo struct {
	Worker
	aaa string
	bbb int
}

func newFooOp(m Mog) Operation {
	return newFoo(m)
}

func newFoo(m Mog) foo {
	f := foo{}
	f.Worker = NewWorker("foo", m)
	return f
}

func fooDoOne(ctx context.Context, g Group[foo], fd foo) (State[foo], foo) {
	fd.aaa = "one"
	fd.bbb++
	if fd.bbb > 10 {
		return g.GetState("done"), fd
	}
	return g.GetState("foo-do-one"), fd

}

func (f foo) GetGroup() Group[foo] {
	gr := NewGroup(newFoo)
	gr.RegisterState("start", fooDoOne)
	return gr
}

func TestOne(t *testing.T) {
	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)
	f := newFoo()
	m := NewMog(g)
	gr := f.GetGroup()
	for op := range m.Deq(f.Name()) {
		opi := op.(foo)
		fd, _ := gr.RunS(ctx, opi)
		t.Logf("fd: %+v", fd)
	}
}
