package pool

// collect concurrently

import (
	"context"
	"sync"
)

func New(ctx context.Context, concurrency int) (*pool, context.CancelFunc) {
	if concurrency < 1 {
		concurrency = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	return &pool{
		concurrency: concurrency,
		ctx:         ctx,
		cancel:      cancel,
	}, cancel
}

type TaskFn func() (interface{}, error)
type ValueFn func(interface{})
type ErrFn func(error)

type pool struct {
	isErr       bool
	concurrency int
	fns         []TaskFn

	ctx    context.Context
	cancel context.CancelFunc
}
type task struct {
	err   error
	value interface{}
}

func (p *pool) WasError() bool {
	return p.isErr
}

func (p *pool) AddTask(t TaskFn) *pool {
	p.fns = append(p.fns, t)
	return p
}

// ValueFn and ErrFn are concurrently safe
// you can write to the same slice without mutexes
func (p *pool) Collect(valueFn ValueFn, errFn ErrFn) {
	results := make(chan task)
	defer p.cancel()
	var resWg sync.WaitGroup
	resWg.Add(1)

	go func() {
		defer resWg.Done()
		for result := range results {
			if result.err != nil {
				p.isErr = true
				if errFn == nil {
					continue
				}

				errFn(result.err)
				continue
			}

			if valueFn == nil {
				continue
			}

			valueFn(result.value)
		}
	}()

	var taskWg sync.WaitGroup
	limiter := make(chan struct{}, p.concurrency)
	for _, fn := range p.fns {
		limiter <- struct{}{} // assign resources

		taskWg.Add(1)
		go func(fn TaskFn) {
			defer func() {
				<-limiter // free resources
				taskWg.Done()
			}()

			select {
			case <-p.ctx.Done(): // canceled

			default:
				value, err := fn()
				results <- task{value: value, err: err}
			}

		}(fn)
	}

	taskWg.Wait()
	close(results)
	resWg.Wait()
}
