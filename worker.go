package safeworker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gzjjyz/logger"
	"github.com/gzjjyz/srvlib/utils"
)

const defCapacity = 50000

const (
	revBatchMsgMaxWait    = time.Millisecond * 5
	loopEventProcInterval = time.Millisecond * 10
)

type msg struct {
	id      MsgIdType
	args    []interface{}
	traceId string
}

type Worker struct {
	name      string
	fetchOnce int
	router    *Router
	chSize    int
	loopFunc  func()
	ch        chan *msg
	wg        sync.WaitGroup
}

func NewWorker(opts ...Option) (*Worker, error) {
	worker := &Worker{}

	for _, opt := range opts {
		opt(worker)
	}

	return worker, worker.init()
}

func (w *Worker) init() error {
	if nil == w.router {
		return errors.New(fmt.Sprintf("worker %s router is nil", w.name))
	}

	if nil == w.loopFunc {
		return errors.New(fmt.Sprintf("worker %s loop func is nil", w.name))
	}

	if 0 >= w.chSize {
		w.chSize = defCapacity
		logger.Warn("worker %s never set ch size. change to defCapacity %d", defCapacity)
	}

	w.ch = make(chan *msg, w.chSize)
	w.fetchOnce = w.chSize / 10

	return nil
}

func (w *Worker) GoStart() error {
	if nil == w.router {
		return errors.New(fmt.Sprintf("worker %s start without any router", w.name))
	}

	err := getMonitor().register(w.name, func() {
		logger.Errorf("worker: %s may offline", w.name)
	})
	if nil != err {
		logger.Errorf("register worker %s to monitor failed error: %v", w.name, err)
		return err
	}
	w.wg.Add(1)

	utils.ProtectGo(func() {
		doLoopFuncTk := time.NewTicker(loopEventProcInterval)

		defer func() {
			w.wg.Done()
			defer doLoopFuncTk.Stop()
		}()

		for {
			select {
			case rec, ok := <-w.ch:
				if !ok {
					return
				}
				w.loop([]*msg{rec})
			case <-doLoopFuncTk.C:
				w.loop(nil)
			}
		}
	},
	)

	return nil
}

func (w *Worker) Close() error {
	close(w.ch)
	w.wg.Wait()
	return nil
}

func (w *Worker) GetRouter() *Router {
	return w.router
}

func (w *Worker) fetchMore(list []*msg) ([]*msg, bool) {
	t := time.Now()
	for {
		select {
		case rec, ok := <-w.ch:
			if !ok {
				return list, true
			}
			list = append(list, rec)
			if len(list) >= w.fetchOnce {
				return list, false
			}
			if since := time.Since(t); since > revBatchMsgMaxWait {
				return list, false
			}
		default:
			return list, false
		}
	}
}

func (w *Worker) loop(list []*msg) (exit bool) {
	list, exit = w.fetchMore(list)

	utils.ProtectRun(w.loopFunc)
	w.router.Process(list)

	getMonitor().report(w.name)
	return
}