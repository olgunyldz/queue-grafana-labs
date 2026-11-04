package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

// FAST (Fonların Anlık ve Sürekli Transferi) event types. Each one has its own
// HTTP endpoint (POST /events/<event>) and its own Kafka topic of the same name.
const (
	EventFastSend        = "fast_send"
	EventFastSendSuccess = "fast_send_success"
	EventFastSendFailed  = "fast_send_failed"
)

var fastEvents = map[string]bool{EventFastSend: true, EventFastSendSuccess: true, EventFastSendFailed: true}

// Transfer is the FAST transfer payload accepted by the endpoints.
type Transfer struct {
	TransactionID string  `json:"transaction_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	SenderIBAN    string  `json:"sender_iban,omitempty"`
	ReceiverIBAN  string  `json:"receiver_iban,omitempty"`
	SenderBank    string  `json:"sender_bank,omitempty"`
	ReceiverBank  string  `json:"receiver_bank,omitempty"`
	Description   string  `json:"description,omitempty"`
	FailureReason string  `json:"failure_reason,omitempty"`
	SentAt        string  `json:"sent_at,omitempty"`
	Source        string  `json:"source,omitempty"`
}

// Event is the envelope written to Kafka.
type Event struct {
	ID            string   `json:"id"`
	Event         string   `json:"event"`
	TransactionID string   `json:"transaction_id"`
	Payload       Transfer `json:"payload"`
	CreatedAt     string   `json:"created_at"`
}

var (
	producedTotal  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "producer_events_total", Help: "FAST events successfully written to Kafka, by event."}, []string{"event"})
	produceErrors  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "producer_errors_total", Help: "Producer errors, by event and reason."}, []string{"event", "reason"})
	producedAmount = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "producer_fast_amount_try_total", Help: "Total TRY amount of FAST events written to Kafka, by event."}, []string{"event"})
	httpRequests   = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "producer_http_requests_total", Help: "HTTP requests to the FAST endpoints, by endpoint and status code."}, []string{"endpoint", "code"})
	httpDuration   = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "producer_http_request_duration_seconds", Help: "HTTP request duration of the FAST endpoints (includes the Kafka write), by endpoint.", Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}}, []string{"endpoint"})
	sequence       uint64
)

// statusRecorder captures the response status code for metrics.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// instrument records request count and duration per FAST endpoint. Unknown
// paths are folded into "unknown" to keep label cardinality bounded.
func instrument(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		endpoint := strings.TrimPrefix(r.URL.Path, "/events/")
		if !fastEvents[endpoint] {
			endpoint = "unknown"
		}
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		next(rec, r)
		httpDuration.WithLabelValues(endpoint).Observe(time.Since(start).Seconds())
		httpRequests.WithLabelValues(endpoint, strconv.Itoa(rec.code)).Inc()
	}
}

func main() {
	prometheus.MustRegister(producedTotal, produceErrors, producedAmount, httpRequests, httpDuration)
	for event := range fastEvents {
		producedTotal.WithLabelValues(event)
		producedAmount.WithLabelValues(event)
	}
	addr := env("HTTP_ADDR", ":8080")
	broker := env("KAFKA_BROKERS", "kafka:9092")

	// No fixed Topic: each message carries its own topic (= event name).
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireOne, BatchTimeout: 10 * time.Millisecond}
	defer writer.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/events/", instrument(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/events/")
		if !fastEvents[name] {
			http.Error(w, "unknown event; expected one of fast_send, fast_send_success, fast_send_failed", http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var transfer Transfer
		if err := json.NewDecoder(r.Body).Decode(&transfer); err != nil {
			produceErrors.WithLabelValues(name, "invalid_json").Inc()
			http.Error(w, "body must be a valid FAST transfer JSON object", http.StatusBadRequest)
			return
		}
		if err := validate(name, &transfer); err != nil {
			produceErrors.WithLabelValues(name, "validation").Inc()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		id := atomic.AddUint64(&sequence, 1)
		event := Event{
			ID:            strconv.FormatUint(id, 10),
			Event:         name,
			TransactionID: transfer.TransactionID,
			Payload:       transfer,
			CreatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		}
		value, err := json.Marshal(event)
		if err == nil {
			err = writer.WriteMessages(r.Context(), kafka.Message{
				Topic:   name,
				Key:     []byte(transfer.TransactionID),
				Value:   value,
				Headers: []kafka.Header{{Key: "event", Value: []byte(name)}},
			})
		}
		if err != nil {
			produceErrors.WithLabelValues(name, "kafka").Inc()
			log.Printf("publish %s failed: %v", name, err)
			http.Error(w, "could not publish event", http.StatusServiceUnavailable)
			return
		}
		producedTotal.WithLabelValues(name).Inc()
		producedAmount.WithLabelValues(name).Add(transfer.Amount)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(event)
	}))

	log.Printf("producer listening on %s; kafka=%s events=fast_send,fast_send_success,fast_send_failed", addr, broker)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func validate(event string, t *Transfer) error {
	if strings.TrimSpace(t.TransactionID) == "" {
		return errors.New("transaction_id is required")
	}
	if t.Amount <= 0 {
		return errors.New("amount must be greater than 0")
	}
	if t.Currency == "" {
		t.Currency = "TRY"
	}
	if t.Currency != "TRY" {
		return errors.New("FAST transfers must be in TRY")
	}
	if event == EventFastSendFailed && strings.TrimSpace(t.FailureReason) == "" {
		return errors.New("failure_reason is required for fast_send_failed")
	}
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
