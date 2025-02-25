// Package state manages, you guessed it, state.
package state

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"io"
	"sync"
	"time"
	// "go.opentelemetry.io/otel/attribute"
	// "go.opentelemetry.io/otel/codes"
)

var (
	DirectEnqueue = false
	EnumError     = errors.New("invalid enum value")
	uuids         = make(chan uuid.UUID)
)

func init() {
	go func() { // error swallowed
		uuidGen()
	}()
}

type OpGen[T any] func() T

type Group[T any] struct {
	toState map[string]State[T]
	pre     []Proc[T]
	post    []Proc[T]
	OpGen[T]
}

func NewGroup[T any](og OpGen[T]) Group[T] {
	g := Group[T]{}
	g.toState = make(map[string]State[T])
	g.OpGen = og
	return g
}

func (g Group[T]) GetState(name string) State[T] {
	return g.toState[name]
}

func (g Group[T]) Pre(ps ...Proc[T]) Group[T] {
	g.pre = append(g.pre, ps...)
	return g
}

func (g Group[T]) Post(ps ...Proc[T]) Group[T] {
	g.post = append(g.post, ps...)
	return g
}

type Method func(context.Context) error

type Step[T any] func(context.Context, Group[T], T) (State[T], T)

type State[T any] struct {
	f    Step[T]
	Name string
}

type Proc[T any] func(context.Context, State[T], T) (T, error)

func proc[T any](ctx context.Context, procs []Proc[T],
	s State[T], payload T) (T, error) {
	var err error
	for _, p := range procs {
		if payload, err = p(ctx, s, payload); err != nil {
			return payload, err
		}
	}
	return payload, nil
}

func stateSet[T Worker](ctx context.Context, s State[T],
	w T) (T, error) {
	w.SetState(s.Name)
	return w, nil
}

func store(m Mog) Proc[Operation] {
	return func(ctx context.Context, s State[Operation],
		o Operation) (Operation, error) {
		m.Rec() <- o
		return o, nil
	}
}

func cont(m Mog) Proc[Operation] {
	return func(ctx context.Context, s State[Operation],
		o Operation) (Operation, error) {
		if !o.Done() && DirectEnqueue {
			m.Enq() <- o
		}
		return o, nil
	}
}

func (g Group[T]) RunS(ctx context.Context, sname string,
	payload T) (T, error) {
	var err error
	for state := g.GetState(sname); state.f != nil; {
		if payload, err = proc(ctx, g.pre, state, payload); err != nil {
			return payload, err
		}
		state, payload = state.f(ctx, g, payload)
		if payload, err = proc(ctx, g.post, state, payload); err != nil {
			return payload, err
		}
	}
	return payload, nil
}

func (g Group[T]) RegisterState(name string, f Step[T]) {
	g.toState[name] = State[T]{f: f, Name: name}
}

type Retry interface {
	Name() string
	Again() bool
	Do(context.Context) error
	Defer() time.Time
}

func uuidGen() error {
	for {
		u, err := uuid.NewV7()
		if err != nil {
			return err
		}
		uuids <- u
	}

}

func NewOp(name string, m Mog) *OpI {
	oc := new(OpI)
	oc.IdI = <-uuids
	oc.NameI = name
	oc.VersionI = "0.0.0" // ? wat means
	oc.DoneI = false
	oc.InflightI = true // on creation is not in DB, is like a bird
	oc.MogI = m
	return oc
}

type OpI struct {
	IdI       uuid.UUID `json:"id"`
	NameI     string    `json:"name"`
	VersionI  string    `json:"version"`
	DoneI     bool      `json:"done"`
	InflightI bool      `json:"inflight"`
	MogI      Mog
}

func (w *OpI) Mog() Mog {
	return w.MogI
}

func (o *OpI) Id() uuid.UUID {
	return o.IdI
}

func (o *OpI) Name() string {
	return o.NameI
}

func (o *OpI) Version() string {
	return o.VersionI
}

func (o *OpI) Done() bool {
	return o.DoneI
}

func (o *OpI) SetDone(d bool) {
	o.DoneI = d
}
func (o *OpI) Inflight() bool {
	return o.InflightI
}

func (o *OpI) SetInflight(i bool) {
	o.InflightI = i
}

type Record interface {
	io.Closer
	Store(context.Context, Operation) error
	Scan(context.Context, chan<- Operation) error
	Get(context.Context, uuid.UUID) (Operation, error)
}

type Pipe interface {
	io.Closer
	Enqueue(context.Context, Operation) error
	Dequeue(context.Context, chan<- Operation) error
}

type Blob interface {
	io.Closer
	Save(context.Context, io.Reader, ...string) error
	Load(context.Context, ...string) (io.Reader, error)
}

type Operation interface {
	Id() uuid.UUID
	Name() string
	Version() string // TODO tighten type
	SetDone(bool)
	Done() bool
	Inflight() bool
	SetInflight(bool)
	Mog() Mog
}

type Worker interface {
	Operation
	State() string // if State[T] then Worker would have to be generic
	SetState(string)
}

type WorkerI struct {
	Operation
	StateI string `json:"state"`
}

func (w *WorkerI) SetState(name string) {
	w.StateI = name
}

func (w *WorkerI) State() string {
	return w.StateI
}

func NewWorker(name string, m Mog) *WorkerI {
	w := new(WorkerI)
	w.Operation = NewOp(name, m)
	return w
}

type mog struct {
	scmutex sync.Mutex
	outs    map[string]chan Operation
	enq     chan Operation
	deq     chan Operation
	rec     chan Operation
	g       *errgroup.Group
	p       Pipe
	r       Record
	b       Blob
	// s.g, ctx = errgroup.WithContext(ctx)
}

func (m mog) Deq(name string) <-chan Operation {
	return m.outs[name]
}

func NewMog(ctx context.Context, g *errgroup.Group, p Pipe,
	r Record, b Blob) Mog { // ? pass in mutex
	m := mog{}
	m.enq = make(chan Operation)
	m.deq = make(chan Operation)
	m.rec = make(chan Operation)
	m.p = p
	m.r = r
	m.b = b
	m.g = g
	m.g.Go(func() error { return rangeRun(ctx, m.enq, m.p.Enqueue) })
	m.g.Go(func() error {
		return tickit(ctx, time.Millisecond*5,
			func() error {
				return m.p.Dequeue(ctx, m.deq)
			})
	})
	m.g.Go(func() error { return rangeRun(ctx, m.rec, m.r.Store) })
	m.g.Go(func() error {
		return tickit(ctx, time.Millisecond*10,
			func() error {
				// ? ingest uniqueness instead of locks
				m.scmutex.Lock()
				defer m.scmutex.Unlock()
				return m.r.Scan(ctx, m.enq)
			})
	})
	m.g.Go(func() error {
		for o := range m.deq {
			m.outs[o.Name()] <- o
		}
		return nil
	})
	return m
}

func (m mog) Rec() chan<- Operation                   { return m.rec }
func (m mog) Enq() chan<- Operation                   { return m.enq }
func (m mog) Check(context.Context, []uuid.UUID) bool { return false }
func (m mog) Get(context.Context, uuid.UUID) (Operation, error) {
	return nil, nil
}

// ? does this need be an interface (was nice for dev)
type Mog interface {
	Rec() chan<- Operation
	Enq() chan<- Operation
	Deq(string) <-chan Operation
	// Check is likely superfluous
	Check(context.Context, []uuid.UUID) bool
	Get(context.Context, uuid.UUID) (Operation, error)
}

func rangeRun(ctx context.Context, ch <-chan Operation,
	f func(ctx context.Context, w Operation) error) error {
	for w := range ch {
		if err := f(ctx, w); err != nil {
			return err
		}
	}
	return nil
}

func tickuntil(ctx context.Context, td time.Duration, f func() bool) error {
	t := time.NewTicker(td)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if f() {
				return nil
			}
		}
	}
}

func tickit(ctx context.Context, td time.Duration, f func() error) error {
	t := time.NewTicker(td)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := f(); err != nil {
				return err
			}
		}
	}
}

/*


type defState[T any] struct {
	scmutex sync.Locker
	b   Blob
	p   Pipe[T]
	r   Record[T]
	g   *errgroup.Group
	enq chan Event[T]
	deq chan Event[T]
	rec chan Event[T]
}

func NewState[T any](ctx context.Context, p Pipe[T], r Record[T], b Blob) (State[T],
		context.Context) {
	s := new(defState[T])
	s.b = b
	s.p = p
	s.r = r
	s.scmutex = new(sync.Mutex) // TODO
	s.enq = make(chan Event[T])
	s.deq = make(chan Event[T])
	s.rec = make(chan Event[T])
	s.g, ctx = errgroup.WithContext(ctx)
	s.g.Go(func() error { return rangeRun(ctx, s.deq,
				func (ctx context.Context, w Event[T]) error {
			w.Next(ctx)
			if directEnqueue && !w.Done(ctx) {
				s.enq <- w
			}
			s.rec <- w
			return nil
		})
	})
	if s.p != nil {
		s.g.Go(func() error { return rangeRun(ctx, s.enq, s.p.Enqueue) })
		s.g.Go(func() error { return tickit(ctx, time.Millisecond*5,
			func () error {
				return s.p.Dequeue(ctx, s.deq)
			})
		})
	}
	if s.r != nil {
		s.g.Go(func() error { return rangeRun(ctx, s.rec, s.r.Store) })
		s.g.Go(func() error { return tickit(ctx, time.Millisecond*10,
			func () error {
				s.scmutex.Lock()
				defer s.scmutex.Unlock()
				return s.r.Scan(ctx, s.enq)
			})
		})
	}
	return s, ctx
}

func (s *defState[T]) Get(ctx context.Context, u uuid.UUID) (Event[T], error) {
	return s.r.Get(ctx, u)
}

func (s *defState[T]) Check(ctx context.Context, ids []uuid.UUID) bool {
	for _, id := range ids {
		w, err := s.Get(ctx, id)
		if err != nil || w == nil { // FIX: timing issue sidestepped.
			return false
		}
		if !w.Done(ctx) {
			return false
		}
	}
	return true
}

func (s *defState[T]) Enq() chan<- Event[T] {
	return s.enq
}

func (s *defState[T]) Close() error {
	if s.b != nil {
		s.b.Close()
	}
	if s.p != nil {
		s.p.Close()
	}
	if s.r != nil {
		s.r.Close()
	}
	return nil
}


// Status enum definition
type Status int

const (
	ENQUEUE Status = iota
	INFLIGHT
	FAIL
	DONE
)

var status2string = map[Status]string{
	ENQUEUE:  "ENQUEUE",
	INFLIGHT: "INFLIGHT",
	FAIL:     "FAIL",
	DONE:     "DONE"}

var string2status = map[string]Status{
	"ENQUEUE":  ENQUEUE,
	"INFLIGHT": INFLIGHT,
	"FAIL":     FAIL,
	"DONE":     DONE}

func (s *Status) MarshalJSON() ([]byte, error) {
	ss := status2string[*s]
	return json.Marshal(ss)
}

func (s Status) String() string {
	return status2string[s]
}

/*
func (s *Status) UnmarshalJSON(b []byte) error {
	var ss string
	var ok bool
	if err := json.Unmarshal(b, &ss); err != nil {
		return err
	}
	*s, ok = string2status[ss]
	if !ok {
		return EnumError
	}
	return nil
}

func (s Status) Value() (driver.Value, error) {
	return status2string[s], nil
}

func (s *Status) Scan(value interface{}) error {
	if value == nil {
		*s = ENQUEUE
		return nil
	}
	switch v := value.(type) {
	case string:
		*s = string2status[v]
	}
	return nil
}

/*
func (e Event[T]) otelStart() Event[T] { // hmm almost a "State"
	if e.span.Span == nil || !e.span.Span.IsRecording() {
		return r
	}
	j, err := json.Marshal(e.T)
	if err != nil {
		j = []byte(fmt.Sprintf("Error marshaling data: %s", err.Error()))
	}
	e.startTime = time.Now()
	// QUESTION about unsafe.String for perf
	e.span.Event(
		"statemachine processing start",
		attribute.String("data", string(j)),
	)
	return r
}

func (e Event[T]) OtelEvent(name string, keyValues ...attribute.KeyValue) {
	if e.span.Span == nil || !e.span.Span.IsRecording() {
		return // No-op
	}
	e.span.Event(name, keyValues...)
}

func (e Event[T]) otelEnd() {
	if e.span.Span == nil || !e.span.Span.IsRecording() {
		return
	}
	if e.Err != nil {
		e.span.Status(codes.Error, e.Err.Error())
		return
	}
	j, err := json.Marshal(e.T)
	if err != nil {
		j = []byte(fmt.Sprintf("Error marshaling data: %s", err.Error()))
	}
	end := time.Now()
	e.Event(
		"statemachine processing end",
		attribute.String("data", string(j)),
		attribute.Int64("elapsed_ns", end.Sub(e.startTime).Nanoseconds()),
	)
	e.span.End()
}
*/
