package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"

	"github.com/example/kafka-grafana-labs/internal/fast"
)

// fastEvent mirrors the envelope written by the producer.
type fastEvent struct {
	ID            string `json:"id"`
	Event         string `json:"event"`
	TransactionID string `json:"transaction_id"`
	Payload       struct {
		Amount        float64 `json:"amount"`
		FailureReason string  `json:"failure_reason"`
		SenderBank    string  `json:"sender_bank"`
		ReceiverBank  string  `json:"receiver_bank"`
		SenderIBAN    string  `json:"sender_iban"`
		ReceiverIBAN  string  `json:"receiver_iban"`
	} `json:"payload"`
}

var (
	consumedTotal       = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_events_total", Help: "FAST events successfully consumed from Kafka, by event."}, []string{"event"})
	consumeErrors       = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_errors_total", Help: "Consumer errors, by reason."}, []string{"reason"})
	consumedAmount      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_fast_amount_try_total", Help: "Total TRY amount of consumed FAST events, by event."}, []string{"event"})
	failedByReason      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_fast_failed_total", Help: "Consumed fast_send_failed events, by failure reason."}, []string{"reason"})
	consumedByPartition = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_partition_events_total", Help: "Events consumed, by consumer group, topic and partition."}, []string{"consumergroup", "topic", "partition"})
	lastOffset          = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "consumer_last_offset", Help: "Offset of the last message consumed, by consumer group, topic and partition."}, []string{"consumergroup", "topic", "partition"})
	bankEvents          = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_fast_bank_events_total", Help: "Consumed FAST events by event, bank role (sender/receiver) and bank."}, []string{"event", "role", "bank_code", "bank"})
	bankAmount          = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_fast_bank_amount_try_total", Help: "TRY amount of consumed FAST events by event, bank role (sender/receiver) and bank."}, []string{"event", "role", "bank_code", "bank"})
	amountHistogram     = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "consumer_fast_amount_try", Help: "Distribution of FAST transfer amounts (TRY), by event.", Buckets: []float64{100, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000}}, []string{"event"})
	endToEndLatency     = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "consumer_event_latency_seconds", Help: "Time between Kafka write and consumption, by event.", Buckets: prometheus.ExponentialBuckets(0.005, 2, 16)}, []string{"event"})
)

func main() {
	prometheus.MustRegister(consumedTotal, consumeErrors, consumedAmount, failedByReason, consumedByPartition, lastOffset, endToEndLatency, bankEvents, bankAmount, amountHistogram)
	broker := env("KAFKA_BROKERS", "kafka:9092")
	topics := strings.Split(env("KAFKA_TOPICS", "fast_send,fast_send_success,fast_send_failed"), ",")
	group := env("KAFKA_GROUP", "fast-transfer-consumers")
	metricsAddr := env("METRICS_ADDR", ":2113")
	pendingThreshold := duration("FAST_PENDING_THRESHOLD", 30*time.Second)
	matchTTL := duration("FAST_MATCH_TTL", 10*time.Minute)
	for _, t := range topics {
		consumedTotal.WithLabelValues(t)
		consumedAmount.WithLabelValues(t)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, GroupTopics: topics, GroupID: group, MinBytes: 1, MaxBytes: 10e6, CommitInterval: time.Second})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	match := newMatcher(pendingThreshold, matchTTL)
	go match.run(ctx.Done(), 5*time.Second)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		log.Printf("consumer metrics listening on %s", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Printf("metrics server stopped: %v", err)
		}
	}()

	log.Printf("consumer started; kafka=%s topics=%s group=%s", broker, strings.Join(topics, ","), group)
	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			consumeErrors.WithLabelValues("read").Inc()
			log.Printf("read error: %v", err)
			continue
		}

		var event fastEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			consumeErrors.WithLabelValues("decode").Inc()
			log.Printf("decode error topic=%s offset=%d: %v", message.Topic, message.Offset, err)
			continue
		}
		name := event.Event
		if name == "" {
			name = message.Topic
		}

		// Business processing for the FAST transfer would go here.
		consumedTotal.WithLabelValues(name).Inc()
		consumedAmount.WithLabelValues(name).Add(event.Payload.Amount)
		if name == "fast_send_failed" {
			failedByReason.WithLabelValues(event.Payload.FailureReason).Inc()
		}
		amountHistogram.WithLabelValues(name).Observe(event.Payload.Amount)
		recordBank(name, "sender", fast.BankCode(event.Payload.SenderBank, event.Payload.SenderIBAN), event.Payload.Amount)
		recordBank(name, "receiver", fast.BankCode(event.Payload.ReceiverBank, event.Payload.ReceiverIBAN), event.Payload.Amount)

		writtenAt := message.Time
		if writtenAt.IsZero() {
			writtenAt = time.Now()
		} else {
			endToEndLatency.WithLabelValues(name).Observe(time.Since(writtenAt).Seconds())
		}
		if event.TransactionID != "" {
			if name == "fast_send" {
				match.send(event.TransactionID, writtenAt)
			} else {
				match.outcome(event.TransactionID, name, writtenAt)
			}
		}
		partition := strconv.Itoa(message.Partition)
		consumedByPartition.WithLabelValues(group, message.Topic, partition).Inc()
		lastOffset.WithLabelValues(group, message.Topic, partition).Set(float64(message.Offset))
		log.Printf("consumed event=%s transaction_id=%s amount=%.2f TRY partition=%d offset=%d", name, event.TransactionID, event.Payload.Amount, message.Partition, message.Offset)
	}
}

func recordBank(event, role, code string, amount float64) {
	if code == "" {
		return
	}
	code, name := fast.BankLabels(code)
	bankEvents.WithLabelValues(event, role, code, name).Inc()
	bankAmount.WithLabelValues(event, role, code, name).Add(amount)
}

func duration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
		log.Printf("invalid %s=%q, using %s", key, value, fallback)
	}
	return fallback
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
