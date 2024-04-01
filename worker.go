package safeworker

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gzjjyz/trace"
	"github.com/petermattis/goid"
	uuid "github.com/satori/go.uuid"

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
	name       string
	fetchOnce  int
	router     *Router
	chSize     int
	loopFunc   func()
	beforeLoop func()
	stopped    atomic.Bool
	afterLoop  func()
	ch         chan *msg
	wg         sync.WaitGroup
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
		logger.LogWarn("worker %s never set ch size. change to defCapacity %d", w.name, defCapacity)
	}

	w.ch = make(chan *msg, w.chSize)
	w.fetchOnce = w.chSize / 10

	logger.LogInfo("worker %s init success", w.name)
	return nil
}

func genUUID() string {
	u2 := uuid.NewV4()
	str := strings.ReplaceAll(u2.String(), "-", "")
	return str[:16]
}

func (w *Worker) getTraceId() string {
	traceId, _ := trace.Ctx.GetCurGTrace(goid.Get())
	if traceId == "" {
		traceId = genUUID()
	} else {
		// 避免太长
		// 避免一个 traceId 贯穿两三个 worker;采用拼接,可以方便的筛选出想看的业务逻辑日志
		split := strings.Split(traceId, ".")
		if len(split) >= 2 {
			traceId = split[0]
		}
		traceId = fmt.Sprintf("%s.%s", traceId, genUUID())
	}
	return traceId
}

func (w *Worker) SendMsg(id MsgIdType, args ...interface{}) {
	if w.stopped.Load() {
		return
	}
	w.ch <- &msg{
		id:      id,
		args:    args,
		traceId: w.getTraceId(),
	}
}

func (w *Worker) GoStart() error {
	if nil == w.router {
		return errors.New(fmt.Sprintf("worker %s start without any router", w.name))
	}

	err := getMonitor().register(w.name, func() {
		var errStr = fmt.Sprintf("worker: %s may offline.", w.name)
		if w.router != nil {
			errStr = fmt.Sprintf("%s%s", errStr, w.router.curMsgInfo())
		}
		logger.LogError(errStr)
	})
	if nil != err {
		logger.LogError("register worker %s to monitor failed error: %v", w.name, err)
		return err
	}
	w.wg.Add(1)

	logger.LogInfo("worker %s GoStart", w.name)

	utils.ProtectGo(func() {
		doLoopFuncTk := time.NewTicker(loopEventProcInterval)

		defer func() {
			w.wg.Done()
			defer doLoopFuncTk.Stop()

			logger.LogInfo("worker %s had exit", w.name)
		}()

		if nil != w.beforeLoop {
			w.beforeLoop()
		}

	EndLoop:
		for {
			select {
			case rec, ok := <-w.ch:
				if !ok {
					break EndLoop
				}
				if w.loop([]*msg{rec}) {
					break EndLoop
				}
			case <-doLoopFuncTk.C:
				if w.loop(nil) {
					break EndLoop
				}
			}
		}

		if nil != w.afterLoop {
			w.afterLoop()
		}
	},
	)

	return nil
}

func (w *Worker) Close() error {
	w.stopped.Store(true)
	close(w.ch)
	w.wg.Wait()

	logger.LogInfo("worker %s close done", w.name)
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
