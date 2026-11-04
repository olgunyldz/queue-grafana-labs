import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    steady_load: { executor: 'constant-arrival-rate', rate: 100, timeUnit: '1s', duration: '60s', preAllocatedVUs: 20, maxVUs: 100 },
  },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<500'] },
};

export default function () {
  const base = __ENV.PRODUCER_URL || 'http://localhost:18081';
  const transfer = {
    transaction_id: `k6-${__VU}-${__ITER}-${Date.now()}`,
    amount: Math.round((10 + Math.random() * 49990) * 100) / 100,
    currency: 'TRY',
    source: 'k6',
    sent_at: new Date().toISOString(),
  };
  const params = { headers: { 'Content-Type': 'application/json' } };
  const send = http.post(`${base}/events/fast_send`, JSON.stringify(transfer), params);
  check(send, { 'fast_send accepted': (r) => r.status === 202 });
  const failed = Math.random() < 0.1;
  const outcome = failed
    ? http.post(`${base}/events/fast_send_failed`, JSON.stringify({ ...transfer, failure_reason: 'INSUFFICIENT_BALANCE' }), params)
    : http.post(`${base}/events/fast_send_success`, JSON.stringify(transfer), params);
  check(outcome, { 'outcome accepted': (r) => r.status === 202 });
}
