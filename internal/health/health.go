// Package health implements a node health checker which can emit events when a
// node goes unhealthy.
package health

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rfratto/croissant/internal/api"
	"github.com/rfratto/croissant/internal/connpool"
)

type metrics struct {
	jobs              prometheus.Gauge
	checksTotal       prometheus.Counter
	failedChecksTotal prometheus.Counter
}

func newMetrics(r prometheus.Registerer) *metrics {
	var m metrics
	m.jobs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "croissant_health_jobs",
		Help: "Current amount of running health cjobs",
	})
	m.checksTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "croissant_health_checks_total",
		Help: "Total number of health checks (succeeded and failed)",
	})
	m.failedChecksTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "croissant_failed_health_checks_total",
		Help: "Total number of failed health checks",
	})

	if r != nil {
		r.MustRegister(m.jobs, m.checksTotal, m.failedChecksTotal)
	}

	return &m
}

func (m *metrics) Unregister(r prometheus.Registerer) {
	if r == nil {
		return
	}
	r.Unregister(m.jobs)
	r.Unregister(m.checksTotal)
	r.Unregister(m.failedChecksTotal)
}

// Watcher is an interface used by Checker to send updates about the health of
// a node as it changes.
type Watcher interface {
	// HealthChanged will be invoked whenever the status of a node changes.
	// Unealthy indicates that the node is suspect and shouldn't be routed
	// to for messages.
	//
	// HealthChanged may be called concurrently.
	HealthChanged(d api.Descriptor, health api.Health)
}

// Config configures how the checker performs.
type Config struct {
	// Frequency to check each node.
	CheckFrequency time.Duration
	// Timeout for each check.
	CheckTimeout time.Duration
	// Maximum number of times a check can fail before the next failure marks as
	// dead. 0 = dead at the first failure.
	MaxFailures int

	Log        log.Logger
	Registerer prometheus.Registerer
}

// Checker is a node health checker. Checker is given a full set of nodes to
// actively perform checks against.
type Checker struct {
	cfg     Config
	pool    *connpool.Pool
	metrics *metrics
	watcher Watcher

	dsChan chan map[string]api.Descriptor

	// Resources protected by a mutex
	mut  sync.RWMutex
	jobs map[string]*job // Currently running jobs. Keyed through return of descriptorKey
	stop chan struct{}   // Close to signal shut down.
	done chan struct{}   // Closed when run exits.
}

// NewChecker creates a new health checker. The pool will be used for
// retrieving gRPC clients. Health change events will be sent to the given
// Watcher.
//
// Checker will run in the background until Close is called.
func NewChecker(cfg Config, p *connpool.Pool, w Watcher) *Checker {
	if cfg.Log == nil {
		cfg.Log = log.NewNopLogger()
	}
	cfg.Log = log.With(cfg.Log, "component", "node_health_checker")

	c := &Checker{
		cfg:     cfg,
		pool:    p,
		watcher: w,
		metrics: newMetrics(cfg.Registerer),

		dsChan: make(chan map[string]api.Descriptor, 1),

		jobs: make(map[string]*job),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}

	go c.run()
	return c
}

func (c *Checker) run() {
	defer close(c.done)

	var wg sync.WaitGroup
	defer wg.Wait()

Outer:
	for {
		select {
		case <-c.stop:
			// Stop all the jobs. This can only happen from Close, where the lock is already held.
			for key, j := range c.jobs {
				level.Debug(c.cfg.Log).Log("msg", "stopping health-tracking for node", "addr", j.cfg.Node.Addr)
				j.Stop()
				delete(c.jobs, key)
			}
			break Outer

		case ds := <-c.dsChan:
			// Sync running jobs with ds.

			// Create a new job if it doesn't exist.
			c.mut.Lock()
			for key, d := range ds {
				if _, found := c.jobs[key]; !found {
					level.Debug(c.cfg.Log).Log("msg", "health-tracking node", "addr", d.Addr)
					c.metrics.jobs.Inc()

					wg.Add(1)
					c.jobs[key] = newJob(jobConfig{
						Pool:        c.pool,
						Node:        d,
						CheckConfig: c.cfg,
						Watcher:     c.watcher,
						Log:         c.cfg.Log,
						Metrics:     c.metrics,
						OnDone: func() {
							c.metrics.jobs.Dec()
							wg.Done()
						},
					})
				}
			}

			// Stop jobs whose descriptors have gone away.
			for key, j := range c.jobs {
				if _, found := ds[key]; !found {
					level.Debug(c.cfg.Log).Log("msg", "stopping health-tracking for node", "addr", j.cfg.Node.Addr)
					j.Stop()
					delete(c.jobs, key)
				}
			}
			c.mut.Unlock()
		}
	}
}

// CheckNodes will update the set of nodes being checked for health. Subsequent
// calls to CheckNodes will stop checking nodes that have been removed from ds
// in between calls.
//
// Fails if the checker is closed.
func (c *Checker) CheckNodes(ds []api.Descriptor) error {
	c.mut.RLock()
	defer c.mut.RUnlock()

	select {
	case <-c.done:
		return fmt.Errorf("Checker closed")
	default:
	}

	dsMap := map[string]api.Descriptor{}
	for _, d := range ds {
		key := descriptorKey(d)
		dsMap[key] = d
	}
	c.dsChan <- dsMap

	return nil
}

func descriptorKey(d api.Descriptor) string {
	return fmt.Sprintf("%s/%s", d.ID.String(), d.Addr)
}

// SetHealth explicitly sets the health of a node and fires off the
// HealthChanged event. This is useful when communicating with a node fails
// and you wish to immediately mark it as suspicious.
func (c *Checker) SetHealth(ds api.Descriptor, h api.Health) error {
	c.mut.RLock()
	defer c.mut.RUnlock()

	if j, ok := c.jobs[descriptorKey(ds)]; ok {
		j.SetHealth(h)
		return nil
	}

	return fmt.Errorf("descriptor not being checked")
}

// Close stops the Checker. Fails if the Checker is already closed.
func (c *Checker) Close() error {
	c.mut.Lock()
	defer c.mut.Unlock()

	select {
	case <-c.done:
		return fmt.Errorf("Checker closed")
	default:
	}

	close(c.stop)
	<-c.done

	c.metrics.Unregister(c.cfg.Registerer)
	return nil
}
