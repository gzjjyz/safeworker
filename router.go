package safeworker

import (
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
		if fn, ok := r.m[line.id]; ok {
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
