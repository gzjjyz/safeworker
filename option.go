package safeworker

type Option func(worker *Worker)

func WithName(name string) Option {
	return func(worker *Worker) {
		worker.name = name
	}
}

func WithChSize(size int) Option {
	return func(worker *Worker) {
		worker.chSize = size
	}
}

func WithRouter(router *Router) Option {
	return func(worker *Worker) {
		worker.router = router
	}
}

func WithLoopFunc(loop func()) Option {
	return func(worker *Worker) {
		worker.loopFunc = loop
	}
}
