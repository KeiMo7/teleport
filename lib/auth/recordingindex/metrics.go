/*
 * Teleport
 * Copyright (C) 2025  Gravitational, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package recordingindex

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/lib/observability/metrics"
)

// Metric labels are restricted to protocol and result per the index contract.
const (
	metricLabelProtocol = "protocol"
	metricLabelResult   = "result"
)

// PrometheusMetrics implements Metrics, DecisionCacheMetrics and
// RetryQueueMetrics.
type PrometheusMetrics struct {
	entries       prometheus.Gauge
	listDuration  *prometheus.HistogramVec
	cacheHits     *prometheus.CounterVec
	cacheMisses   *prometheus.CounterVec
	invalidations prometheus.Counter
	retryDropped  prometheus.Counter
	backendErrors *prometheus.CounterVec
}

// NewPrometheusMetrics constructs and registers the index collectors.
func NewPrometheusMetrics() *PrometheusMetrics {
	m := &PrometheusMetrics{
		entries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: teleport.MetricNamespace,
			Subsystem: "recordingindex",
			Name:      "entries",
			Help:      "Current number of recording index entries.",
		}),
		listDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: teleport.MetricNamespace,
			Subsystem: "recordingindex",
			Name:      "list_duration_seconds",
			Help:      "Duration of recording index List calls.",
			Buckets:   prometheus.DefBuckets,
		}, []string{metricLabelProtocol, metricLabelResult}),
		cacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: teleport.MetricNamespace,
			Subsystem: "recordingindex",
			Name:      "authz_cache_hits_total",
			Help:      "Number of recording index authorization cache hits.",
		}, []string{metricLabelProtocol, metricLabelResult}),
		cacheMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: teleport.MetricNamespace,
			Subsystem: "recordingindex",
			Name:      "authz_cache_misses_total",
			Help:      "Number of recording index authorization cache misses.",
		}, []string{metricLabelProtocol, metricLabelResult}),
		invalidations: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: teleport.MetricNamespace,
			Subsystem: "recordingindex",
			Name:      "authz_invalidations_total",
			Help:      "Number of recording index authorization cache entries invalidated.",
		}),
		retryDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: teleport.MetricNamespace,
			Subsystem: "recordingindex",
			Name:      "retry_dropped_total",
			Help:      "Number of failed index writes dropped from the bounded retry queue.",
		}),
		backendErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: teleport.MetricNamespace,
			Subsystem: "recordingindex",
			Name:      "backend_errors_total",
			Help:      "Number of recording index backend errors.",
		}, []string{metricLabelResult}),
	}
	metrics.RegisterPrometheusCollectors(
		m.entries,
		m.listDuration,
		m.cacheHits,
		m.cacheMisses,
		m.invalidations,
		m.retryDropped,
		m.backendErrors,
	)
	return m
}

// EntryGauge implements Metrics.
func (m *PrometheusMetrics) EntryGauge(count int) {
	m.entries.Set(float64(count))
}

// ObserveList implements Metrics.
func (m *PrometheusMetrics) ObserveList(protocol string, duration time.Duration) {
	if protocol == "" {
		protocol = "unknown"
	}
	m.listDuration.WithLabelValues(protocol, "ok").Observe(duration.Seconds())
}

// ObserveBackendError implements Metrics. Only the restricted result label is
// used, keeping all index metrics within the allowed label set.
func (m *PrometheusMetrics) ObserveBackendError(operation string) {
	m.backendErrors.WithLabelValues("error").Inc()
}

// CacheHit implements DecisionCacheMetrics.
func (m *PrometheusMetrics) CacheHit(protocol string) {
	m.cacheHits.WithLabelValues(nonEmpty(protocol), "allow").Inc()
}

// CacheMiss implements DecisionCacheMetrics.
func (m *PrometheusMetrics) CacheMiss(protocol string) {
	m.cacheMisses.WithLabelValues(nonEmpty(protocol), "unknown").Inc()
}

// Invalidation implements DecisionCacheMetrics.
func (m *PrometheusMetrics) Invalidation(count int) {
	m.invalidations.Add(float64(count))
}

// RetryDropped implements RetryQueueMetrics.
func (m *PrometheusMetrics) RetryDropped() {
	m.retryDropped.Inc()
}

func nonEmpty(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}
