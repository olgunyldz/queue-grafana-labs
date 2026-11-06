# Apache Flink Reconciler Gereksinimleri

## 1. Kapsam

Apache Flink, mevcut [reconciler](./reconciler/) servisine alternatif olarak ayrı bir uygulama şeklinde geliştirilecektir. Mevcut Kafka Streams reconciler silinmeyecek, değiştirilmeyecek ve geçiş süresince çalışabilir durumda kalacaktır.

Yeni uygulama ayrı bir klasörde oluşturulmalıdır:

```text
flink-reconciler/
```

## 2. Kafka giriş ve çıkış sözleşmesi

Flink job aşağıdaki topic'leri tüketmelidir:

- `fast_send`
- `fast_send_success`
- `fast_send_failed`

Event'ler `transaction_id` alanına göre gruplanmalıdır. Aşağıdaki output topic'leri korunmalıdır:

- `fast_transfer_matched`
- `fast_transfer_unmatched`

Shadow doğrulama için aşağıdaki topic'ler kullanılabilir:

- `fast_transfer_matched_flink_shadow`
- `fast_transfer_unmatched_flink_shadow`

## 3. Reconciliation davranışı

Flink job mevcut reconciler davranışını korumalıdır:

- `fast_send` ile success veya failed event'i `transaction_id` üzerinden eşleştirilmelidir.
- Event'lerin geliş sırası önemli olmamalıdır; outcome önce gelirse state'te tutulmalıdır.
- Eşleşen event `fast_transfer_matched` topic'ine yazılmalıdır.
- Eşleşmeyen event 10 dakika sonra `fast_transfer_unmatched` topic'ine yazılmalıdır.
- 30 saniyeyi aşan bekleyen transfer stuck olarak ölçülmelidir.
- Eşleşen kayıtta amount, sender bank, receiver bank, outcome ve completion latency bulunmalıdır.
- Eşleşme sonrası ilgili keyed state temizlenmelidir.
- İlk uygulamada mevcut davranışla uyumluluk için processing-time timer kullanılmalıdır.

## 4. State ve dayanıklılık

- `transaction_id` keyed state kullanılmalıdır.
- State backend restart sonrasında geri yüklenebilmelidir.
- Checkpoint alınmalıdır.
- Checkpoint'ler kalıcı bir volume veya harici dayanıklı storage üzerinde tutulmalıdır.
- Kafka output sink exactly-once semantiğini desteklemelidir.
- Job restart sonrasında duplicate matched veya unmatched output oluşmamalıdır.

## 5. Flink çalışma ortamı

Docker Compose ortamına aşağıdaki bileşenler eklenmelidir:

- Flink JobManager
- En az iki Flink TaskManager
- Flink job image / job submission mekanizması
- Prometheus metrics reporter

Flink job Java 21 ile geliştirilmeli ve sürümler Docker image ile Maven yapılandırmasında sabitlenmelidir.

## 6. Prometheus ve Grafana metrikleri

Flink Prometheus reporter etkinleştirilmelidir. En az aşağıdaki teknik metrikler Prometheus'a açılmalıdır:

- job status
- records in / records out
- Kafka source lag
- checkpoint duration
- checkpoint failure
- restart count
- TaskManager CPU ve memory kullanımı

İş metrikleri mevcut dashboard uyumluluğunu korumalıdır:

- `fast_reconciler_transfers_total`
- `fast_reconciler_unmatched_total`
- `fast_reconciler_pending`
- `fast_reconciler_stuck`
- `fast_reconciler_completion_seconds`
- `fast_reconciler_amount_try`

Grafana dashboard'larında ayrıca Flink job, checkpoint ve TaskManager durumu gösterilmelidir.

## 7. Test gereksinimleri

### Unit test

- send önce, outcome sonra
- outcome önce, send sonra
- success eşleşmesi
- failed eşleşmesi
- duplicate event
- 30 saniye stuck
- 10 dakika unmatched
- aynı transaction için birden fazla outcome
- state recovery

### Integration test

- Kafka topic'leri ile gerçek input/output akışı
- checkpoint sonrası restart
- TaskManager kapanması
- Flink job restart
- offset devamlılığı
- exactly-once output doğrulaması

### Performans ve alarm testleri

Artillery içine aşağıdaki senaryolar eklenmelidir:

- `flink-open`
- `flink-job-down`
- `flink-backlog`
- `flink-restart-with-state`
- `flink-checkpoint-failure`
- `missing-outcomes-with-flink`

Testler Prometheus API üzerinden metric ve lag doğrulaması yapabilmelidir.

## 8. Shadow ve geçiş gereksinimleri

1. Flink ilk aşamada shadow topic'lere yazmalıdır.
2. Kafka Streams ve Flink çıktıları `transaction_id` üzerinden karşılaştırılmalıdır.
3. Matched, unmatched, duplicate, latency, banka ve tutar sonuçları eşitlenmelidir.
4. Kafka Streams reconciler ile Flink aynı anda production output topic'lerine yazmamalıdır.
5. Doğrulama tamamlanmadan mevcut [reconciler](./reconciler/) kaldırılmamalıdır.
6. Cutover sonrasında eski reconciler rollback için korunmalıdır.

## 9. Kabul kriterleri

- 900 input transferin 900'ü doğru şekilde eşleşmelidir.
- Stuck ve unmatched süreleri mevcut davranışla aynı olmalıdır.
- Restart sonrasında state kaybolmamalıdır.
- Exactly-once output ile duplicate oluşmamalıdır.
- Flink job durumu Prometheus ve Grafana'da görünmelidir.
- Mevcut FAST dashboard'ları Flink metrikleriyle çalışmalıdır.
- Kafka Streams reconciler kapalıyken Flink reconciler tek başına çalışmalıdır.
