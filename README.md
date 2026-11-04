# FAST Transfer Events: Kafka Producer / Consumer + Grafana

C# (.NET 10) ile yazılmış bir FAST (Fonların Anlık ve Sürekli Transferi) event akışı örneği. Producer bir HTTP API sunar ve FAST event'lerini Kafka'ya yazar; consumer `fast-transfer-consumers-dotnet` consumer group'u ile bu event'leri tüketir. Producer ve consumer 2'şer replica ile çalışır. Prometheus metrikleri Grafana dashboard'larında gösterilir.

## FAST event'leri

| Event | Endpoint | Kafka topic | Anlamı |
|---|---|---|---|
| `fast_send` | `POST /events/fast_send` | `fast_send` | FAST transferi gönderildi |
| `fast_send_success` | `POST /events/fast_send_success` | `fast_send_success` | Transfer başarıyla tamamlandı |
| `fast_send_failed` | `POST /events/fast_send_failed` | `fast_send_failed` | Transfer başarısız oldu (`failure_reason` zorunlu) |

Her event kendi topic'ine (3 partition) yazılır; message key `transaction_id`'dir. Body zorunlu alanları: `transaction_id`, `amount` (> 0) ve `currency` (`TRY`, verilmezse varsayılan). Bilinmeyen event adı `404`, hatalı body `400` döner.

## Çalıştırma

Docker ve Docker Compose kurulu olmalıdır:

```bash
docker compose up --build
```

Servisler:

- Producer API: http://localhost:18081 (nginx load balancer → 2 producer replica)
- Consumer metrikleri: http://localhost:2114/metrics ve http://localhost:2115/metrics (replica başına bir port)
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- Loki: http://localhost:3100
- kafka-exporter: http://localhost:9308/metrics

Event üretmek için:

```bash
curl -X POST http://localhost:18081/events/fast_send \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"tx-1001","amount":1250.75,"currency":"TRY","sender_bank":"00010","receiver_bank":"00046"}'

curl -X POST http://localhost:18081/events/fast_send_success \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"tx-1001","amount":1250.75,"sender_bank":"00010","receiver_bank":"00046"}'

curl -X POST http://localhost:18081/events/fast_send_failed \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"tx-1002","amount":99.90,"failure_reason":"INSUFFICIENT_BALANCE"}'
```

## Dashboard'lar

Dashboard'lar klasörlere göre gruplanmıştır. Her dashboard'un sağ üstündeki **FAST lab** menüsünden diğerlerine geçilebilir (zaman aralığı korunur).

| Klasör | Dashboard | Kimin için | İçerik |
|---|---|---|---|
| **FAST** | **FAST Operations** | İş / operasyon | Özet, hacim ve alert eşikleri, tamamlanma süresi, bankalar, para ve hata nedenleri |
| **FAST** | **FAST Services** | Uygulama ekibi | Producer API, consumer replica'ları, runtime ve loglar |
| **Kafka** | **Kafka Consumer Groups & Broker** | Platform | Consumer group lag'i, partition detayları, broker sağlığı |

Panellerin ayrıntısı:

- **FAST Operations:**
  - **Özet:** transfer sayısı, başarı oranı, tamamlanma süresi p95, takılı transferler (> 30 sn), başarılı ve başarısız TRY tutarları.
  - **Hacim ve alert eşikleri:** `FAST alerts` listesi ve hacim grafikleri. Grafiklerde alert eşikleri kesikli çizgi, mesai saatleri gölge olarak görünür; geçen haftanın aynı saatiyle karşılaştırılır.
  - **Tamamlanma:** `fast_send` → sonuç süresi (p50/p95/p99 ve heatmap), sonuç bekleyen, takılı ve hiç sonuç almamış transferler.
  - **Bankalar:** gönderen/alıcı seçilebilir. Banka tablosu, TRY hacim payı, en kötü 5 bankanın hata oranı.
  - **Para ve hatalar:** dakikalık TRY hacmi, ortalama tutar, tutar dağılımı, hata nedenleri.
- **FAST Services:**
  - **Replica'lar:** ayakta olan producer/consumer replica sayısı, group üye sayısı, API hata oranı ve p95.
  - **Producer API:** endpoint/status bazında istek/s, p95/p99, replica başına istek (load balancer dağılımı), 4xx/5xx oranı, hata nedenleri.
  - **Consumer replica'ları:** replica başına tüketim, replica'ların okuduğu partition'lar, Kafka → consumer gecikmesi, eşleştirme durumu, hatalar.
  - **Runtime:** replica başına GC heap, working set, CPU, GC sayısı.
  - **Loglar:** hata/uyarı log sayısı ve logları, servis ve regex filtreli tüm loglar.
- **Kafka Consumer Groups & Broker:**
  - **Consumer group:** üye sayısı, toplam lag, tüketme ve üretme hızı, lag'in tahmini erime süresi, topic bazında lag.
  - **Partition'lar:** partition başına lag ve tüketim hızı, group üyeliğinin zaman içindeki değişimi, offset/lag tablosu.
  - **Broker:** broker ve controller durumu, offline / under-replicated partition'lar, topic başına mesaj/s, byte in/out, Produce/Fetch istek süresi p99.

Consumer group üyeliği değiştiğinde (restart/rebalance) grafiklerde mor işaret görünür.

Grafana'nın Loki data source'u da otomatik provision edilir; container logları `Explore` ekranından görülebilir.

### Bankalar

Banka kodları [Banks.cs](./dotnet/FastTransfer.Shared/Banks.cs) içindedir. Kod, `sender_bank`/`receiver_bank` alanından okunur; bu alanlar boşsa IBAN'ın 5–9. hanelerinden alınır.

- Listede olmayan kodlar `other` olarak raporlanır. Bu, metrik etiket sayısının sınırsız büyümesini önler.
- Enpara.com, QNB altyapısını ve 00111 kodunu kullandığı için IBAN'dan ayırt edilemez; ikisi `QNB / Enpara.com` olarak birlikte raporlanır.

### Transfer eşleştirme

Consumer, `fast_send` ile sonucunu `transaction_id` üzerinden bellekte eşleştirir. Süre, Kafka'ya yazılma zamanlarından hesaplanır. Bu sayede sonuç, send'den önce okunsa da süre doğru çıkar.

| Ortam değişkeni | Varsayılan | Anlamı |
|---|---|---|
| `FAST_PENDING_THRESHOLD` | `30s` | Bu süreden uzun sonuç bekleyen transfer "takılı" sayılır |
| `FAST_MATCH_TTL` | `10m` | Bu süre içinde eşleşmeyen kayıt düşürülür ve `fast_transfers_expired_total` artar |

Eşleştirme durumu her replica'nın kendi belleğindedir:

- **Doğru replica:** bir transferin bütün event'leri aynı replica'ya düşer (aşağıdaki **Ölçekleme** bölümüne bakın).
- **Restart / rebalance:** consumer yeniden başladığında veya partition'lar başka replica'ya geçtiğinde, yarım kalan eşleşmeler taşınmaz. TTL dolunca "expired" olarak raporlanır.

## Servisler ve ölçekleme

Kod [dotnet/](./dotnet/) altındadır ([FastTransfer.slnx](./dotnet/FastTransfer.slnx)):

| Proje | İçerik | Dockerfile |
|---|---|---|
| `FastTransfer.Shared` | FAST modelleri, banka listesi, ortam değişkeni yardımcıları | — (kütüphane) |
| `FastTransfer.Producer` | HTTP API → Kafka | [FastTransfer.Producer/Dockerfile](./dotnet/FastTransfer.Producer/Dockerfile) |
| `FastTransfer.Consumer` | Kafka → metrikler, `transaction_id` eşleştirmesi | [FastTransfer.Consumer/Dockerfile](./dotnet/FastTransfer.Consumer/Dockerfile) |

Compose'da her iki servis de `deploy.replicas: 2` ile çalışır:

- **Producer (`dotnet-producer` ×2):** önünde `producer-lb` (nginx) vardır ve tek giriş noktası http://localhost:18081'dir. nginx, Docker DNS'i 5 saniyede bir yeniden çözer ve istekleri replica'lar arasında dağıtır. Yanıttaki `X-Upstream` header'ı, isteği hangi replica'nın karşıladığını gösterir.
- **Consumer (`dotnet-consumer` ×2):** iki replica aynı consumer group'tadır (`fast-transfer-consumers-dotnet`), Kafka partition'ları aralarında paylaştırır. Bir transferin `fast_send`'i ile sonucunun aynı replica'ya düşmesi için iki koşul vardır:
  1. Producer bütün event'leri `transaction_id` key'i ile yazar. Topic'lerin partition sayısı aynıdır (3), bu yüzden aynı transfer her topic'te aynı partition numarasına düşer.
  2. Consumer **range** assignor kullanır. Bu assignor, her replica'ya her topic'te aynı partition numaralarını verir (ör. replica 1 → `[2]`, replica 2 → `[0, 1]`).

  Topic'lere farklı partition sayısı verilirse bu garanti bozulur.
- **Prometheus:** her replica'yı Docker DNS üzerinden (`dns_sd_configs`) ayrı ayrı scrape eder. Dashboard'lar replica'ları toplar; runtime panelleri replica başına çizgi gösterir.

Replica sayısını değiştirmek için `deploy.replicas` değerini güncelleyin. Consumer için host port aralığını (`2114-2115`) da büyütün. Partition sayısından (3) fazla consumer replica'sı boşta kalır.

```bash
docker compose up -d --build dotnet-producer dotnet-consumer producer-lb
```

### Eski consumer group

Projenin ilk sürümü Go ile yazılmıştı; git geçmişinde duruyor. O sürümün consumer group'u (`fast-transfer-consumers`) mevcut Kafka verisinde hâlâ duruyorsa lag'i büyür. Silmek için:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group fast-transfer-consumers
```

## FAST alert'leri

Alert'ler Grafana Alerting ile `FAST` klasöründe tanımlıdır ([fast-alerts.yml](./observability/grafana/provisioning/alerting/fast-alerts.yml)) ve `fast-transfer-consumers-dotnet` consumer group'u (tüm consumer replica'ları birlikte) için çalışır. Tüm sayılar group'un **son 5 dakikada** tükettiği event'lerdir. Kaynak, commit edilen Kafka offset'leridir (`kafka-exporter`), bu yüzden consumer restart'larından etkilenmez.

| Alert | Koşul | Ne zaman | Severity | Süre |
|---|---|---|---|---|
| FAST send volume low (business hours) | `fast_send` < 100 | 09:00–18:00 (İstanbul) | warning | 5 dk |
| FAST send volume high | `fast_send` > 6000 | Her zaman | critical | 5 dk |
| FAST outcome volume low (business hours) | `success + failed` < 100 | 09:00–18:00 (İstanbul) | warning | 5 dk |
| FAST outcome volume high | `success + failed` > 6000 | Her zaman | critical | 5 dk |
| FAST failed count high | `fast_send_failed` > 500 | Her zaman | critical | 5 dk |
| FAST failed/success ratio high | `failed / success` > %20 | Her zaman | critical | 5 dk |
| FAST send/outcome mismatch | `\|send − outcome\| / send` > %10 | Her zaman | warning | 5 dk |

- **Eşikleri değiştirmek:** [fast-alerts.yml](./observability/grafana/provisioning/alerting/fast-alerts.yml) içinde ilgili kuralın `params` değerini (ve `for` süresini) değiştirip Grafana'yı yeniden başlatın.
- **Saat aralığını değiştirmek:** [rules/fast.yml](./observability/prometheus/rules/fast.yml) içindeki `fast:business_hours` kuralında `9` / `18` değerlerini değiştirin (UTC+3 sabit). Ardından `docker compose kill -s HUP prometheus` çalıştırın.
- **Oranların hesaplanma şartı:** 5 dakikada 20 event'in altında oranlar hesaplanmaz, böylece düşük trafikte yanlış alarm oluşmaz.
- **Consumer kapalıyken:** düşük hacim alert'leri mesai saatlerinde tetiklenir, çünkü group hiçbir şey tüketmez.
- **Kullanılan recording rule'lar:** `fast:sent:increase5m`, `fast:outcome:increase5m`, `fast:success:increase5m`, `fast:failed:increase5m`, `fast:failed_success:ratio5m`, `fast:outcome_mismatch:ratio5m`, `fast:business_hours`.
- **Bildirim:** Slack, e-posta vb. için Grafana'da bir contact point tanımlanmalıdır. Şu an alert'ler yalnızca Grafana'da ve dashboard'daki `FAST alerts` panelinde görünür.

## Hazır imaj sürümleri

Compose hazır imajları güncel kararlı sürümlere sabitlenmiştir: Apache Kafka 4.4.0, Prometheus 3.13.1, Grafana 13.3.0, Loki 3.7.9 ve Grafana Alloy 1.21.3. Uygulama imajları .NET 10 SDK / ASP.NET Core runtime ile derlenir; producer load balancer'ı nginx 1.29.3'tür.

## Performans testi

 [k6](https://k6.io/) kuruluysa:

```bash
k6 run performance/producer.js
```

Varsayılan test 60 saniye boyunca saniyede 100 transfer üretir (her biri `fast_send` + `%90 fast_send_success` / `%10 fast_send_failed`). API adresini değiştirmek için `PRODUCER_URL` kullanılabilir:

```bash
PRODUCER_URL=http://localhost:18081 k6 run performance/producer.js
```

Artillery case'leri için [performance/artillery/README.md](./performance/artillery/README.md) dosyasına bakın. Producer host portu `18081`'dir.

## Metrikler

- `producer_events_total{event}`: Kafka'ya başarıyla yazılan FAST event sayısı
- `consumer_events_total{event}`: consumer tarafından okunan FAST event sayısı
- `producer_fast_amount_try_total{event}`, `consumer_fast_amount_try_total{event}`: TRY tutar toplamları
- `consumer_fast_failed_total{reason}`: başarısız transferlerin nedene göre dağılımı
- `consumer_event_latency_seconds{event}`: Kafka'ya yazılma ile tüketilme arasındaki süre
- `consumer_partition_events_total`, `consumer_last_offset`: partition bazında tüketim
- `producer_errors_total{event,reason}`, `consumer_errors_total{reason}`: hata sayaçları
- `producer_http_requests_total{endpoint,code}`, `producer_http_request_duration_seconds{endpoint}`: API istek sayısı ve süresi
- `fast_transfer_completion_seconds{outcome}`: `fast_send` → sonuç süresi
- `fast_transfers_pending`, `fast_transfers_pending_over_threshold`, `fast_outcomes_without_send`, `fast_transfers_expired_total{kind}`, `fast_duplicate_events_total{event}`: eşleştirme durumu
- `consumer_fast_bank_events_total{event,role,bank_code,bank}`, `consumer_fast_bank_amount_try_total{...}`: banka bazında sayı ve TRY tutar
- `consumer_fast_amount_try{event}`: tutar dağılımı (histogram)
- `kafka_consumergroup_lag`, `kafka_consumergroup_current_offset`, `kafka_topic_partition_current_offset`, `kafka_consumergroup_members`: `kafka-exporter` (http://localhost:9308/metrics) üzerinden consumer group metrikleri

Uygulama sayaçları process yeniden başladığında sıfırlanır; kalıcı toplamlar için Kafka offset metrikleri kullanılır. Consumer'da gerçek iş mantığı yoktur; işleme adımı consumer döngüsüne eklenebilir.
