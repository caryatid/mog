package pipe

import (
	"context"
	//	"encoding/json"
	"github.com/caryatid/cueball"
	"github.com/caryatid/cueball/state"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

// TODO think on this
var subname = "cueball.worker"

type natsp struct {
	nats *nats.Conn
	sub  *nats.Subscription
	g    *errgroup.Group
	msg  chan *nats.Msg
}

func NewNats(ctx context.Context, natsurl string) (cueball.Pipe, error) {
	var err error
	p := new(natsp)
	p.g, ctx = errgroup.WithContext(ctx)
	p.nats, err = nats.Connect(natsurl)
	if err != nil {
		return nil, err
	}
	p.msg = make(chan *nats.Msg)
	p.sub, err = p.nats.QueueSubscribe(subname, subname, func(m *nats.Msg) {
		p.msg <- m
	})
	if err != nil {
		return nil, err
	}
	return p, err
}

func (p *natsp) Close() error {
	p.sub.Drain()
	p.sub.Unsubscribe()
	return p.nats.Drain()
}

func (p *natsp) Enqueue(ctx context.Context, w cueball.Worker) error {
	data, err := state.Marshal(w)
	if err != nil {
		return err
	}
	pk := &state.Pack{Name: w.Name(), Codec: string(data)}
	wdata, err := state.Marshal(pk)
	if err != nil {
		return err
	}
	return p.nats.Publish(subname, wdata)
}

func (p *natsp) Dequeue(ctx context.Context, ch chan<- cueball.Worker) error {
	msg := <-p.msg
	pk := new(state.Pack)
	if err := state.Unmarshal(string(msg.Data), pk); err != nil {
		return err
	}
	w := cueball.GenWorker(pk.Name)
	if err := state.Unmarshal(pk.Codec, w); err != nil {
		return err
	}
	ch <- w
	return nil
}
