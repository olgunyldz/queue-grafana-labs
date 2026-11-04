package main

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// matcher pairs each fast_send with its outcome (fast_send_success or
// fast_send_failed) by transaction_id.
//
// The three topics are consumed in parallel, so an outcome can be read before
// its send (e.g. while a fast_send backlog drains). Both sides are therefore
// buffered, and the completion time is computed from the Kafka write
// timestamps, not from the order in which the messages are read.
//
// State is in memory only: after a consumer restart, transfers that were
// half-matched are reported as expired once the TTL passes.
type matcher struct {
	mu        sync.Mutex
	sends     map[string]time.Time
	outcomes  map[string]pendingOutcome
	threshold time.Duration // a send older than this without an outcome is "stuck"
	ttl       time.Duration // unmatched entries are dropped after this
}

type pendingOutcome struct {
	event string
	at    time.Time
}

var (
	completionTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fast_transfer_completion_seconds",
		Help:    "Time from fast_send to its outcome (Kafka write timestamps), by outcome.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 120, 300, 600},
	}, []string{"outcome"})
	pendingTransfers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fast_transfers_pending",
		Help: "fast_send events waiting for an outcome.",
	})
	stuckTransfers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fast_transfers_pending_over_threshold",
		Help: "fast_send events waiting for an outcome longer than the threshold (FAST_PENDING_THRESHOLD, default 30s).",
	})
	outcomesWithoutSend = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fast_outcomes_without_send",
		Help: "Outcome events whose fast_send has not been seen (yet).",
	})
	expiredTransfers = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fast_transfers_expired_total",
		Help: "Unmatched entries dropped after the TTL (FAST_MATCH_TTL, default 10m), by kind.",
	}, []string{"kind"})
	duplicateEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fast_duplicate_events_total",
		Help: "Events whose transaction_id was already waiting on the same side, by event.",
	}, []string{"event"})
)

func init() {
	prometheus.MustRegister(completionTime, pendingTransfers, stuckTransfers, outcomesWithoutSend, expiredTransfers, duplicateEvents)
	for _, o := range []string{"fast_send_success", "fast_send_failed"} {
		completionTime.WithLabelValues(o)
	}
	for _, k := range []string{"send_without_outcome", "outcome_without_send"} {
		expiredTransfers.WithLabelValues(k)
	}
}

func newMatcher(threshold, ttl time.Duration) *matcher {
	return &matcher{sends: map[string]time.Time{}, outcomes: map[string]pendingOutcome{}, threshold: threshold, ttl: ttl}
}

func (m *matcher) send(tx string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o, ok := m.outcomes[tx]; ok {
		delete(m.outcomes, tx)
		observeCompletion(o.event, o.at.Sub(at))
		return
	}
	if _, ok := m.sends[tx]; ok {
		duplicateEvents.WithLabelValues("fast_send").Inc()
		return
	}
	m.sends[tx] = at
}

func (m *matcher) outcome(tx, event string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sentAt, ok := m.sends[tx]; ok {
		delete(m.sends, tx)
		observeCompletion(event, at.Sub(sentAt))
		return
	}
	if _, ok := m.outcomes[tx]; ok {
		duplicateEvents.WithLabelValues(event).Inc()
		return
	}
	m.outcomes[tx] = pendingOutcome{event: event, at: at}
}

func observeCompletion(outcome string, d time.Duration) {
	if d < 0 {
		d = 0
	}
	completionTime.WithLabelValues(outcome).Observe(d.Seconds())
}

// sweep updates the gauges and drops entries older than the TTL.
func (m *matcher) sweep(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stuck := 0
	for tx, at := range m.sends {
		age := now.Sub(at)
		switch {
		case age > m.ttl:
			delete(m.sends, tx)
			expiredTransfers.WithLabelValues("send_without_outcome").Inc()
		case age > m.threshold:
			stuck++
		}
	}
	for tx, o := range m.outcomes {
		if now.Sub(o.at) > m.ttl {
			delete(m.outcomes, tx)
			expiredTransfers.WithLabelValues("outcome_without_send").Inc()
		}
	}
	pendingTransfers.Set(float64(len(m.sends)))
	stuckTransfers.Set(float64(stuck))
	outcomesWithoutSend.Set(float64(len(m.outcomes)))
}

func (m *matcher) run(stop <-chan struct{}, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			m.sweep(now)
		}
	}
}
