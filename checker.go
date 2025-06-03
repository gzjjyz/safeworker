package safeworker

import (
	"time"
)

type TimeChecker struct {
	interval time.Duration //间隔
	next     time.Time     //下次时间戳
}

func NewTimeChecker(interval time.Duration) *TimeChecker {
	checker := new(TimeChecker)
	checker.interval = interval
	checker.next = time.Now().Add(interval)
	return checker
}

func (checker *TimeChecker) Check() bool {
	return checker.next.After(time.Now())
}

func (checker *TimeChecker) Next() time.Time {
	return checker.next
}

func (checker *TimeChecker) CheckAndSet(ignore bool) bool {
	now := time.Now()
	if checker.next.After(now) {
		return false
	}
	if ignore {
		checker.next = now.Add(checker.interval)
	} else {
		checker.next = checker.next.Add(checker.interval)
	}
	return true
}
