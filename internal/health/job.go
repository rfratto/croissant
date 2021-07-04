package health

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/rfratto/croissant/internal/api"
	"github.com/rfratto/croissant/internal/connpool"
	"github.com/rfratto/croissant/internal/nodepb"
)

type jobConfig struct {
	// Client Pool
	Pool *connpool.Pool
	// Node to check
	Node api.Descriptor
	// Logging
	Log log.Logger
	// Metrics for jobs
	Metrics *metrics
	// Config for checks
	CheckConfig Config
	// Watcher to notify when state changes.
	Watcher Watcher
	// OnDone will be called when the Job closes.
	OnDone func()
}

type job struct {
	cfg  jobConfig
	done chan struct{}

	mut            sync.Mutex
	health         api.Health
	failedAttempts int
}

// newJob creates and starts a health check job. Call Stop to finish.
func newJob(c jobConfig) *job {
	j := &job{
		cfg:    c,
		health: api.Healthy,
		done:   make(chan struct{}),
	}
	go j.run()
	return j
}

func (j *job) run() {
	defer j.cfg.OnDone()

	t := time.NewTicker(j.cfg.CheckConfig.CheckFrequency)
	defer t.Stop()

	for {
		select {
		case <-j.done:
			return
		case <-t.C:
			j.doCheck()
		}
	}
}

func (j *job) doCheck() {

	ctx, cancel := context.WithTimeout(context.Background(), j.cfg.CheckConfig.CheckTimeout)
	defer cancel()

	// Grab a client from the conn pool
	cc, err := j.cfg.Pool.Get(j.cfg.Node.Addr)
	if err != nil {
		level.Debug(j.cfg.Log).Log("msg", "creating client for node health check failed", "err", err)
		j.processCheckResult(false)
		return
	}

	cli := nodepb.NewNodeClient(cc)
	_, err = cli.GetState(ctx, &nodepb.GetStateRequest{})
	if err != nil {
		level.Debug(j.cfg.Log).Log("msg", "node health check failed", "err", err)
	}
	j.processCheckResult(err == nil && ctx.Err() == nil)
}

func (j *job) processCheckResult(success bool) {
	j.cfg.Metrics.checksTotal.Inc()
	if !success {
		j.cfg.Metrics.failedChecksTotal.Inc()
	}

	switch {
	case success:
		j.SetHealth(api.Healthy)

	case !success && j.failedAttempts < j.cfg.CheckConfig.MaxFailures:
		// If we've failed but there are still more attempts remaining, move to unhealthy.
		j.failedAttempts++
		j.SetHealth(api.Unhealthy)

	default:
		// If we've exhausted our attempts, move to dead.
		j.SetHealth(api.Dead)
	}
}

// SetHealth explicitly sets the health the job.
func (j *job) SetHealth(h api.Health) {
	j.mut.Lock()
	defer j.mut.Unlock()

	// Ignore if the health matches or if it's an invalid state transition.
	// Dead can go to Healthy, but not Unhealthy.
	if j.health == h || j.health == api.Dead && h == api.Unhealthy {
		return
	}

	// Reset failed attempts in case SetHealth was called manually; otherwise
	// there's a chance a single failure will go straight to Dead.
	if h == api.Healthy {
		j.failedAttempts = 0
	}

	j.health = h

	// Call HealthChanged in background so we can continue running checks.
	go j.cfg.Watcher.HealthChanged(j.cfg.Node, h)
}

// Stop stops the job. Only call once.
func (j *job) Stop() {
	close(j.done)
}
