# workers

Simple waiting workers implementation. Here are:

SimpleWorker - one thread worker, that sleeps until time elapsed or it is not forced
AsyncWorker - multi-thread worker, that sleeps until time elapsed or it is not forced

AsyncWorker runs N goroutines, defined by runnerFunc.

OnStart and OnFinish funcs are not necessary. runnerFunc is necessary - if not defined workers panics on job starting.