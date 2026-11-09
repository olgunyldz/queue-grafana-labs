# Apache Flink Reconciler Gereksinimleri

> **Not:** Bu doküman Flink geçişinin **ilk** gereksinimleridir ve tarihçe için olduğu gibi korunmuştur. Aşağıdaki metin hâlâ eski `fast_send` topic'lerinden ve `flink-fast-reconciler/` klasöründen bahseder. Güncel yapı için [README](./README.md), [flink/reconciler/README.md](./flink/reconciler/README.md) ve [dashboard.md](./dashboard.md) esas alınır.

## Uygulama durumu (2026-11-08)

✅ karşılandı · 🔄 farklı şekilde karşılandı · ⚠️ kısmen · ❌ yapılmadı

| Gereksinim | Durum | Not |
|---|---|---|
| Ayrı Flink uygulaması, Kafka Streams korunur | 🔄 | Geçiş sonrasında Kafka Streams reconciler kaldırıldı (kodu git geçmişinde); Flink tek reconciler |
| Klasör `flink-fast-reconciler/` | 🔄 | Tek proje `flink/reconciler/`; FAST, Havale ve EFT aynı kodla, akış başına ayrı job ve cluster (`FLOW`) |
| Topic'ler `fast_send*` | 🔄 | Yönlü topic'ler: `fast_{incoming,outgoing}_{request,success,failed}`, `havale_*`, `eft_*` |
| Output `fast_transfer_matched / unmatched` | ✅ | Akış başına `<akış>_transfer_matched / unmatched` |
| Shadow topic'ler | ❌ | Flink doğrudan çıktı topic'lerine yazar |
| Eşleştirme davranışı (§3) | ✅ | Ek olarak: TTL kaydın state'e girişinden sayılır (backlog'da sahte unmatched yok); eşleşmiş ID'ler 10 dk tutulur (geç gelen duplicate sayılır) |
| Keyed state, RocksDB, checkpoint, exactly-once sink (§4) | ✅ | Checkpoint'ler `volumes/flink-checkpoints/<akış>` |
| Restart sonrası state | ✅ | JobManager restart'ında `docker/start-job.sh` son checkpoint'ten devam eder; TaskManager / Kafka kesintisinde Flink kendi içinde geri yükler (elle doğrulandı) |
| JobManager, Prometheus reporter (§5) | ✅ | Application mode; ayrı submit adımı yok |
| En az iki TaskManager | ❌ | Akış başına bir TaskManager (4 slot, paralellik 3, bir yedek slot) |
| Teknik metrikler (§6) | ✅ | Platform · Flink ve akış SRE panoları |
| İş metrik adları `fast_reconciler_*` | 🔄 | `reconciler_*{flow}` |
| Unit test: send/outcome sırası, success, failed, 10 dk unmatched, duplicate, state recovery | ✅ | `FlinkReconciliationFunctionTest`, `KafkaInputEventDeserializerTest`, `FlowTest` |
| Unit test: 30 s stuck, aynı transaction için birden fazla outcome | ⚠️ | Mantık var (stuck timer, duplicate yolu); ayrı test yok |
| Integration testler | ⚠️ | Otomatik değil; checkpoint'ten restart ve Kafka kesintisi elle doğrulandı |
| Artillery `flink-*` senaryoları | ⚠️ | Ayrı `flink-*` case'leri yok; `request-backlog`, `missing-outcomes` vb. Flink üzerinde çalışır |
| Kafka Streams kapalıyken Flink tek başına çalışır | ✅ | Kafka Streams kaldırıldı; Flink tek reconciler |

---

## 1. Kapsam

Apache Flink, mevcut [reconciler](./reconciler/) servisine alternatif olarak ayrı bir uygulama şeklinde geliştirilecektir. Mevcut Kafka Streams reconciler silinmeyecek, değiştirilmeyecek ve geçiş süresince çalışabilir durumda kalacaktır.

Yeni uygulama ayrı bir klasörde oluşturulmalıdır:

```text
flink-fast-reconciler/
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
