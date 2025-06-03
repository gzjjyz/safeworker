package safeworker

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	v2 "github.com/gzjjyz/srvlib/utils/signal/v2"
)

var (
	once      sync.Once
	singleton *monitor
)

type description struct {
	counter      uint32
	timeOutCb    func()
	checker      *TimeChecker
	startTimeOut atomic.Int64
}

type monitor struct {
	workers      sync.Map
	exit         chan struct{}
	warnAlarm    func(msg string)
	successAlarm func(msg string)
}

func getMonitor() *monitor {
	once.Do(func() {
		singleton = &monitor{
			workers: sync.Map{},
			exit:    make(chan struct{}, 1),
		}
	})

	return singleton
}

func GetMonitor() *monitor {
	return getMonitor()
}

func (m *monitor) RegGlobalTimeOutCb(f func(errMsg string)) {
	if f == nil {
		return
	}
	m.warnAlarm = f
}

func (m *monitor) RegGlobalSuccessCb(f func(msg string)) {
	if f == nil {
		return
	}
	m.successAlarm = f
}

func (m *monitor) stop() {
	m.exit <- struct{}{}
	close(m.exit)
}

func (m *monitor) register(workerName string, onTimeOutCb func()) error {
	if _, ok := m.workers.Load(workerName); ok {
		return fmt.Errorf("worker %s already registered", workerName)
	}

	wd := description{
		counter:   0,
		timeOutCb: onTimeOutCb,
	}

	m.workers.Store(workerName, &wd)

	return nil
}

func (m *monitor) report(workerName string) {
	w, ok := m.workers.Load(workerName)
	if !ok {
		return
	}
	if work, ok := w.(*description); ok {
		atomic.StoreUint32(&work.counter, 0)
		if start := work.startTimeOut.Load(); start != 0 {
			if m.successAlarm != nil {
				m.successAlarm(fmt.Sprintf("持续时间(秒)：%d", time.Now().Unix()-start))
			}
			work.startTimeOut.Store(0)
		}
	}
}

func (m *monitor) run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.exit:
			return
		case <-ticker.C:
			m.workers.Range(func(name, value interface{}) bool {
				wd := value.(*description)
				if atomic.AddUint32(&wd.counter, 1) > 5 {
					if start := wd.startTimeOut.Load(); start == 0 {
						wd.startTimeOut.Store(time.Now().Unix())
					}
					if wd.timeOutCb == nil {
						return true
					}
					wd.timeOutCb()
					if m.warnAlarm != nil {
						if wd.checker != nil && !wd.checker.CheckAndSet(true) {
							return true
						}
						if wd.checker == nil {
							wd.checker = NewTimeChecker(time.Second * 5)
						}
						m.warnAlarm(fmt.Sprintf("worker %v timeout", name))
					}
				}
				return true
			})
		}
	}
}

func init() {
	go func() {
		v2.OnSign(func(o os.Signal) {
			getMonitor().stop()
		}, syscall.SIGINT, syscall.SIGKILL, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGQUIT)
		getMonitor().run()
	}()
}
