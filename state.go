// package mog mogs u
package mog

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
	gens = make(map[string]OpGen)
)

func init() {
	go func() { // ! error swallowed
		uuidGen()
	}()
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

type OpGen func() Operation

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

func NewOp(name string, m Mog) *opI {
	oc := new(opI)
	oc.IdI = <-uuids
	oc.NameI = name
	oc.VersionI = "0.0.0" // ? wat means
	oc.DoneI = false
	oc.InflightI = true // on creation is not in DB, is like a bird
	oc.MogI = m
	return oc
}

type opI struct {
	IdI       uuid.UUID `json:"id"`
	NameI     string    `json:"name"`
	VersionI  string    `json:"version"`
	DoneI     bool      `json:"done"`
	InflightI bool      `json:"inflight"`
	DeferI    time.Time `json:"defer"`
	MogI      Mog
}

func (w *opI) Mog() Mog {
	return w.MogI
}

func (o *opI) Id() uuid.UUID {
	return o.IdI
}

func (o *opI) Name() string {
	return o.NameI
}

func (o *opI) Version() string {
	return o.VersionI
}

func (o *opI) Done() bool {
	return o.DoneI
}

func (o *opI) SetDone(d bool) {
	o.DoneI = d
}

func (o *opI) Inflight() bool {
	return o.InflightI
}

func (o *opI) SetInflight(i bool) {
	o.InflightI = i
}

func (o *opI) Defer() time.Time {
	return o.DeferI
}

func (o *opI) SetDefer(t time.Time) {
	o.DeferI = t
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
	Defer() time.Time
	SetDefer(time.Time)
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
	gens    map[string]OpGen
	enq     chan Operation
	deq     chan Operation
	rec     chan Operation
	g       *errgroup.Group
	p       Pipe
	r       Record
	b       Blob
}

func (m mog) Deq(name string) <-chan Operation {
	return m.outs[name]
}

func NewMog(ctx context.Context, g *errgroup.Group, p Pipe,
	r Record, b Blob) Mog { // ? pass in mutex
	m := mog{}
	m.outs = make(map[string]chan Operation)
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

func RegisterOp(f OpGen) {
	gens[f().Name()] = f
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
