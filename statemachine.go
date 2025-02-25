package mog

type Step[T any] func(context.Context, Group[T], T) (State[T], T)

type Proc[T any] func(context.Context, State[T], T) (T, error)

type State[T any] struct {
	f    Step[T]
	Name string
}

type Group[T any] struct {
	stateMap map[string]State[T]
	pre     []Proc[T]
	post    []Proc[T]
}

func NewGroup[T any]() Group[T] {
	g := Group[T]{}
	g.stateMap = make(map[string]State[T])
	return g
}

func (g Group[T]) GetState(name string) State[T] {
	return g.stateMap[name]
}

func (g Group[T]) Pre(ps ...Proc[T]) Group[T] {
	g.pre = append(g.pre, ps...)
	return g
}

func (g Group[T]) Post(ps ...Proc[T]) Group[T] {
	g.post = append(g.post, ps...)
	return g
}

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
	g.stateMap[name] = State[T]{f: f, Name: name}
}
