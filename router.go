package safeworker

import (
	"sync"
	"time"

	"github.com/gzjjyz/logger"
	"github.com/gzjjyz/srvlib/utils"
	"github.com/gzjjyz/trace"
	"github.com/petermattis/goid"
)

const DefSlowTime = 20 * time.Millisecond

type MsgIdType uint32
type MsgHdlType func(param ...interface{})

type Router struct {
	m        map[MsgIdType]MsgHdlType
	slowTime time.Duration
	mu       sync.RWMutex
}

func NewRouter(slow time.Duration) *Router {
	if slow == 0 {
		slow = DefSlowTime
	}
	return &Router{
		m:        make(map[MsgIdType]MsgHdlType),
		slowTime: slow,
	}
}

func (r *Router) Register(id MsgIdType, cb MsgHdlType) {
	if nil == cb {
		logger.Fatalf("worker router callback is nil, id=%v", id)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	_, repeat := r.m[id]
	if repeat {
		logger.Fatalf("worker router register repeated. id=%d", id)
		return
	}
	r.m[id] = cb
}

func (r *Router) Process(list []*msg) {
	gid := goid.Get()
	for _, line := range list {
		t := time.Now()
		r.mu.RLock()
		fn, ok := r.m[line.id]
		r.mu.RUnlock()
		if ok {
			trace.Ctx.SetCurGTrace(gid, line.traceId)
			utils.ProtectRun(func() {
				fn(line.args[:]...)
			})
		}
		if since := time.Since(t); since > r.slowTime {
			logger.Debug("process msg end! id:%v, cost:%v", line.id, since)
		}
	}
}
