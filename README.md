# FAST Transfer Events Lab

Bu proje, FAST transfer event’lerinin Kafka üzerinden taşınmasını, iki farklı reconciliation uygulamasıyla eşleştirilmesini ve Grafana’da gözlemlenmesini gösteren yerel bir laboratuvar ortamıdır.

Projeyi yeni açan bir geliştirici için en önemli fikir şudur:

> Producer `fast_send` event’ini yayınlar. Daha sonra aynı `transaction_id` için başarı veya hata sonucu gelir. Kafka Streams ve Flink reconciler’ları bu iki event’i eşleştirir, sonucu topic’lere yazar ve iş metrikleri üretir.

## İçindekiler

- [Mimariyi hızlıca anlama](#mimariyi-hızlıca-anlama)
- [Servisler](#servisler)
- [Event akışı](#event-akışı)
- [Projeyi çalıştırma](#projeyi-çalıştırma)
- [Test transferi gönderme](#test-transferi-gönderme)
- [Reconciler’lar](#reconcilerlar)
- [Grafana ve gözlemlenebilirlik](#grafana-ve-gözlemlenebilirlik)
- [Prometheus metrikleri](#prometheus-metrikleri)
- [Kalıcı volume klasörleri](#kalıcı-volume-klasörleri)
- [Proje yapısı](#proje-yapısı)
- [Performans testleri](#performans-testleri)
- [Geliştirme ve test](#geliştirme-ve-test)
- [Sorun giderme](#sorun-giderme)

## Mimariyi hızlıca anlama

```mermaid
flowchart LR
    Client["Client / Artillery / k6"] --> LB["producer-lb\n127.0.0.1:8080"]
    LB --> P["mock-producer\n2 replica"]
    P --> K[(Kafka)]

    K --> C["mock-consumer\n2 replica"]
    K --> KS["fast-reconciler\nKafka Streams"]
    K --> F["FAST Flink Reconciler\nFlink job"]

    KS --> O1[("fast_transfer_matched\nfast_transfer_unmatched")]
    F --> O2[("fast_transfer_matched\nfast_transfer_unmatched")]

    P -. metrics .-> PM[Prometheus]
    C -. metrics .-> PM
    KS -. metrics .-> PM
    F -. metrics .-> PM
    K -. exporter metrics .-> PM
    PM --> G[Grafana]
    P -. logs .-> A[Alloy]
    C -. logs .-> A
    KS -. logs .-> A
    A --> L[Loki]
    L --> G
```

Kafka Streams ve Flink reconciler’ları birbirinin yerine kullanılabilir iki ayrı uygulamadır. Karşılaştırma yapılabilmesi için ikisi de aynı input topic’lerini okur, aynı output topic sözleşmesini izler ve FAST iş metriklerini aynı mantıksal isimlerle üretir.

## Servisler

| Servis | Teknoloji | Görevi | Adres / port |
|---|---|---|---|
| `kafka` | Apache Kafka 4.4 | Event ve output topic’lerini taşır | `localhost:9092` |
| `kafka-init` | Kafka CLI | Gerekli topic’leri oluşturur | Tek seferlik servis |
| `mock-producer` | .NET 10 | HTTP event API’si ve Kafka producer | İç port `8080` |
| `producer-lb` | nginx | Producer replica’ları için tek giriş noktası | `http://127.0.0.1:8080` |
| `mock-consumer` | .NET 10 | Input event’lerini tüketen örnek consumer | `2114`, `2115` |
| `fast-reconciler` | Java 21 / Kafka Streams | FAST event’lerini eşleştirir | `9400`, `9401` metrics |
| `flink-checkpoint-init` | Flink image | Flink image’ını build eder, akış başına checkpoint klasörlerini hazırlar | Tek seferlik servis |
| `flink-fast-jobmanager` | Apache Flink 2.1.3 | FAST cluster’ı; FAST job’unu application mode’da çalıştırır | `http://localhost:18082` |
| `flink-havale-jobmanager` | Apache Flink 2.1.3 | Havale cluster’ı; Havale job’unu çalıştırır | `http://localhost:18083` |
| `flink-eft-jobmanager` | Apache Flink 2.1.3 | EFT cluster’ı; EFT job’unu çalıştırır | `http://localhost:18084` |
| `flink-<flow>-taskmanager` | Apache Flink 2.1.3 | İlgili akışın task’lerini çalıştırır (4 slot) | Metrics portu `9249` |
| `kafka-exporter` | kafka-exporter | Consumer group lag ve offset metrikleri | `9308` |
| `kafka-jmx-exporter` | JMX exporter | Kafka broker metrikleri | `5556` |
| `prometheus` | Prometheus | Metrikleri scrape eder ve saklar | `http://localhost:9090` |
| `loki` | Loki | Logları saklar | `http://localhost:3100` |
| `alloy` | Grafana Alloy | Docker loglarını Loki’ye gönderir | Internal |
| `grafana` | Grafana | Dashboard ve alert ekranı | `http://localhost:3000` |

`mock-producer`, `mock-consumer` ve Kafka Streams reconciler’ı Compose içinde iki replica ile çalışır. Flink tarafında her akışın (FAST, Havale, EFT) kendi cluster’ı vardır: bir JobManager ve bir TaskManager. Üç cluster da aynı image’ı ([flink/reconciler/](./flink/reconciler/)) kullanır; hangi akışın çalışacağını `FLOW` ortam değişkeni belirler. State, checkpoint, consumer group, Kafka transaction id’leri ve metrik endpoint’i her akış için ayrıdır.

## Event akışı

### Input topic’leri

| Event | HTTP endpoint | Kafka topic | Anlamı |
|---|---|---|---|
| `fast_incoming_success` | `POST /events/fast/incoming/success` | `fast_incoming_success` | FAST gelen başarılı |
| `fast_incoming_failed` | `POST /events/fast/incoming/failed` | `fast_incoming_failed` | FAST gelen başarısız |
| `fast_outgoing_success` | `POST /events/fast/outgoing/success` | `fast_outgoing_success` | FAST giden başarılı |
| `fast_outgoing_failed` | `POST /events/fast/outgoing/failed` | `fast_outgoing_failed` | FAST giden başarısız |
| `havale_success` | `POST /events/havale/success` | `havale_success` | Havale başarılı |
| `havale_failed` | `POST /events/havale/failed` | `havale_failed` | Havale başarısız |
| `eft_incoming_success` | `POST /events/eft/incoming/success` | `eft_incoming_success` | EFT gelen başarılı |
| `eft_incoming_failed` | `POST /events/eft/incoming/failed` | `eft_incoming_failed` | EFT gelen başarısız |
| `eft_outgoing_success` | `POST /events/eft/outgoing/success` | `eft_outgoing_success` | EFT giden başarılı |
| `eft_outgoing_failed` | `POST /events/eft/outgoing/failed` | `eft_outgoing_failed` | EFT giden başarısız |

Her event’in Kafka message key’i `transaction_id` değeridir. Mock consumer’lar kendi consumer group’larına ait event’leri okuyup loglar.

### Output topic’leri

| Topic | İçerik |
|---|---|
| `fast_transfer_matched` | `fast_send` ile başarı/hata sonucunun eşleştiği transferler |
| `fast_transfer_unmatched` | TTL sonunda eşleşemeyen transferler |

## Projeyi çalıştırma

Gereksinimler:

- Docker Desktop
- Docker Compose v2
- Performans testleri için Node.js ve npm
- Docker dışında Java testleri için JDK 21 ve Maven

Tüm stack’i başlatmak için:

```powershell
docker compose up -d --build
```

Durumu kontrol etmek için:

```powershell
docker compose ps
docker compose logs -f fast-reconciler
docker compose logs -f flink-fast-jobmanager
```

Flink job listesini görmek için (Havale `18083`, EFT `18084`):

```powershell
Invoke-RestMethod http://localhost:18082/jobs/overview
```

Flink UI: FAST [http://localhost:18082](http://localhost:18082), Havale [http://localhost:18083](http://localhost:18083), EFT [http://localhost:18084](http://localhost:18084)

Grafana varsayılan giriş bilgileri:

- Kullanıcı: `admin`
- Şifre: `admin`

Grafana anonim erişime de açıktır; dashboard’ları [http://localhost:3000](http://localhost:3000) üzerinden açabilirsiniz.

## Test transferi gönderme

Örneğin FAST gelen başarılı bir event gönderin:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8080/events/fast/incoming/success `
  -ContentType 'application/json' `
  -Body '{"transaction_id":"tx-1001","amount":1250.75,"currency":"TRY","sender_bank":"00010","receiver_bank":"00046"}'
```

FAST giden başarısız event için:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8080/events/fast/outgoing/failed `
  -ContentType 'application/json' `
  -Body '{"transaction_id":"tx-1001","amount":1250.75,"sender_bank":"00010","receiver_bank":"00046"}'
```

Havale başarılı event için aynı JSON sözleşmesini kullanın:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8080/events/havale/success `
  -ContentType 'application/json' `
  -Body '{"transaction_id":"tx-1002","amount":500,"currency":"TRY","sender_bank":"00010","receiver_bank":"00046","failure_reason":"INSUFFICIENT_BALANCE"}'
```

Başarılı HTTP kabulü `202`, geçersiz body `400`, bilinmeyen event `404`, Kafka yazma hatası `503` döndürür.

## Reconciler’lar

### Kafka Streams reconciler

Kaynak: [reconciler/](./reconciler/)

Kafka Streams uygulaması `fast_send`, `fast_send_success` ve `fast_send_failed` topic’lerini okur. State, `transaction_id` ile keyed store’da tutulur. RocksDB local state’i Kafka changelog topic’i ile desteklenir; bu nedenle instance yeniden başlatıldığında state tekrar oluşturulabilir.

Önemli ayarlar:

| Environment variable | Varsayılan | Açıklama |
|---|---:|---|
| `KAFKA_BROKERS` | `kafka:9092` | Kafka adresi |
| `APPLICATION_ID` | `fast-reconciler` | Kafka Streams application ID / consumer group |
| `FAST_PENDING_THRESHOLD` | `30s` | Bu süreden uzun bekleyen transfer stuck sayılır |
| `FAST_MATCH_TTL` | `10m` | Bu sürede eşleşmeyen transfer unmatched olur |
| `FAST_SCAN_INTERVAL` | `5s` | State tarama sıklığı |
| `NUM_STANDBY_REPLICAS` | `1` | Standby task sayısı |
| `PROCESSING_GUARANTEE` | `exactly_once_v2` | Kafka Streams processing guarantee |
| `REPARTITION_PARTITIONS` | `3` | Internal repartition topic partition sayısı |

### Flink reconciler

Kaynak: [flink/reconciler/](./flink/reconciler/)

Tek kod tabanı, akış başına ayrı job ve ayrı cluster. Job’un entry point’i [FlinkReconcilerJob.java](./flink/reconciler/src/main/java/fast/reconciler/flink/FlinkReconcilerJob.java), akış varsayılanları [Flow.java](./flink/reconciler/src/main/java/fast/reconciler/flink/Flow.java) dosyasındadır. Job şu adımları izler:

1. Akışın request, success ve failed topic’lerini Kafka source ile okur.
2. `transaction_id` ile keyBy yapar.
3. `FlinkReconciliationFunction` içinde event’leri eşleştirir.
4. `<flow>_transfer_matched` veya `<flow>_transfer_unmatched` topic’ine exactly-once sink ile yazar.
5. 10 saniyelik checkpoint ile state’i `volumes/flink-checkpoints/<flow>` klasörüne kaydeder.

| Akış (`FLOW`) | Input topic’leri | Output topic’leri | Consumer group / job adı | UI |
|---|---|---|---|---|
| `fast` | `fast_{incoming,outgoing}_{request,success,failed}` | `fast_transfer_matched`, `fast_transfer_unmatched` | `flink-fast-reconciler` | `18082` |
| `havale` | `havale_{request,success,failed}` | `havale_transfer_matched`, `havale_transfer_unmatched` | `flink-havale-reconciler` | `18083` |
| `eft` | `eft_{incoming,outgoing}_{request,success,failed}` | `eft_transfer_matched`, `eft_transfer_unmatched` | `flink-eft-reconciler` | `18084` |

Flink ortam değişkenleri (JobManager servisine verilir; application mode’da job orada başlar):

| Environment variable | Varsayılan | Açıklama |
|---|---:|---|
| `FLOW` | `fast` | `fast`, `havale` veya `eft`; aşağıdaki varsayılanları seçer |
| `KAFKA_BROKERS` | `kafka:9092` | Kafka adresi |
| `APPLICATION_ID` | `flink-<flow>-reconciler` | Job adı, consumer group ve transactional id öneki |
| `SEND_TOPIC`, `SUCCESS_TOPIC`, `FAILED_TOPIC` | akışa göre | Virgülle ayrılmış input topic’leri |
| `MATCHED_TOPIC`, `UNMATCHED_TOPIC` | `<flow>_transfer_matched` / `_unmatched` | Output topic’leri |
| `PENDING_THRESHOLD` | `30s` | Stuck eşiği |
| `MATCH_TTL` | `10m` | Unmatched TTL; eşleşen kayıtların tekrar kontrolü de bu süre kadar tutulur |
| `CHECKPOINT_INTERVAL` | `10s` | Checkpoint aralığı |
| `PARALLELISM` | `3` | Flink job parallelism’i |

Süreler `500ms`, `30s`, `10m`, `1h` biçiminde verilir; geçersiz bir değerde job açık bir hata mesajıyla başlamaz.

Flink job’unu kontrol etmek için:

```powershell
Invoke-RestMethod http://localhost:18082/jobs/overview
```

Beklenen job adı `flink-<flow>-reconciler`, beklenen durum `RUNNING` olmalıdır (her akışın kendi portunda).

## Grafana ve gözlemlenebilirlik

Grafana dashboard’ları dosyadan provision edilir. Elle dashboard oluşturmak yerine JSON dosyasını (ekip panolarında generator script’ini) güncellemek gerekir.

### Ekip panoları (FAST, Havale, EFT)

Her akışın kendi Grafana klasörü ve her klasörde üç ekibe göre hazırlanmış pano vardır. Panoların üstündeki bağlantılarla aynı akışın diğer panolarına veya tüm akışların panolarına geçilebilir.

| Pano | Kimin için | Ne gösterir |
|---|---|---|
| `<Akış> · İş Birimi` | İş birimi, ürün sahipleri | Tamamlanan transfer, başarı oranı, başarılı TRY hacmi, ortalama tutar, tamamlanma süresi, sonuçsuz kalan istekler; dakikalık trend (geçen haftayla karşılaştırmalı); gelen/giden kırılımı (FAST, EFT); banka özeti ve en yüksek hata oranlı bankalar (gönderen / alıcı seçilebilir); başarısızlık nedenleri; akışın iş uyarıları |
| `<Akış> · Geliştirici` | Geliştirme ekibi | API istek hızı, gecikme ve hata kodları; topic bazında Kafka yazma hızı ve reconciler lag’i; mock consumer gecikmesi; reconciler iç durumu (eşleşen / unmatched, pending / stuck / orphan, duplicate ve geçersiz event, tamamlanma süresi histogramı); Flink task throughput ve sink hataları; Flink ve API hata logları (Loki) |
| `<Akış> · SRE` | SRE / operasyon | JobManager, TaskManager, çalışan job, uptime, restart, boş slot, lag, checkpoint yaşı; SLO’lar (sonuca ulaşan istek ≥ %99,9, 5 s içinde tamamlanma ≥ %99, API başarı ≥ %99,9) ve bilgi amaçlı iş kaynaklı başarısızlık oranı; throughput, backpressure, lag, p99; checkpoint süresi / boyutu / hataları; CPU, heap, GC; akışın tüm uyarıları ve hata log sayısı |

Bu 9 pano ve Platform · Servisler / Flink panoları [generate_team_dashboards.py](./observability/grafana/generate_team_dashboards.py) ile üretilir. Panoyu değiştirmek için JSON yerine script’i düzenleyip çalıştırın:

```powershell
python observability/grafana/generate_team_dashboards.py
docker compose restart grafana   # Windows bind mount’ta dosya değişikliği otomatik algılanmayabilir
```

### Platform panoları

| Klasör | Dashboard | Ne için kullanılır |
|---|---|---|
| `Platform` | Platform · Servisler | Mock API ve consumer replikaları: replika sayısı, yük dağılımı, gecikme, .NET runtime, loglar |
| `Platform` | Platform · Flink (tüm akışlar) | Üç Flink cluster’ı yan yana: job, slot, checkpoint, backpressure, lag, kaynaklar |
| `Platform` | Platform · Kafka | Consumer group lag, partition ve broker sağlığı |

Panoların ayrıntılı açıklaması, alarmlar ve demo akışı için [dashboard.md](./dashboard.md).

### Hangi dashboard’a bakılmalı?

- Bir akışın iş sonuçları (adet, başarı, hacim, banka): ilgili klasördeki `İş Birimi` panosu.
- Event akışında hata, gecikme, duplicate veya eşleşmeyen kayıt: `Geliştirici` panosu.
- Servis ayakta mı, SLO’lar tutuyor mu, checkpoint / lag / kaynak sorunu var mı: `SRE` panosu.
- Hangi akışın Flink cluster’ı sorunlu: `Platform · Flink`.
- API / consumer replikaları veya Kafka broker problemi: `Platform · Servisler`, `Platform · Kafka`.

İş metrikleri `reconciler_*` adını ve `flow` etiketini kullanır. Kafka Streams reconciler (`kstreams` profili, yalnızca FAST) aynı metrikleri `impl="kstreams"` ile üretir; ayrı bir panosu yoktur, gerekirse Explore’dan sorgulanır.

### Alert listesi

Transfer alarmları [transfer-alerts.yml](./observability/grafana/provisioning/alerting/transfer-alerts.yml) (`Transfers` klasörü), Flink altyapı alarmları [flink-alerts.yml](./observability/grafana/provisioning/alerting/flink-alerts.yml) (`Flink` klasörü) dosyasındadır. Her alarm **akış başına ayrı instance** üretir; bildirimdeki `flow` etiketi hangi akışın etkilendiğini gösterir. Eşikler şu an tüm akışlar için aynıdır.

| Alert | Koşul | Severity | Firing süresi | Kaynak |
|---|---|---|---|---|
| `Transfer request/outcome mismatch` | Son 5 dakikada request ve outcome farkı `%10` üstü | warning | 5 dakika | `flink-<flow>-reconciler` offset’leri |
| `Transfer request volume low (business hours)` | İstanbul mesai saatlerinde son 5 dakikada `100`’den az request | warning | 5 dakika | Kafka consumer group offset’leri |
| `Transfer request volume high` | Son 5 dakikada `6000`’den fazla request | critical | 5 dakika | Kafka consumer group offset’leri |
| `Transfer outcome volume low (business hours)` | İstanbul mesai saatlerinde son 5 dakikada `100`’den az outcome | warning | 5 dakika | Kafka consumer group offset’leri |
| `Transfer outcome volume high` | Son 5 dakikada `6000`’den fazla outcome | critical | 5 dakika | Kafka consumer group offset’leri |
| `Transfer failed count high` | Son 5 dakikada `500`’den fazla failed outcome | critical | 5 dakika | Kafka consumer group offset’leri |
| `Transfer failed/success ratio high` | `failed / success` oranı `%20` üstü | critical | 5 dakika | Kafka consumer group offset’leri |
| `Transfer stuck transfers high` | 30 saniyeden uzun bekleyen `50`’den fazla transfer | warning | 5 dakika | `reconciler_stuck` (Flink ve Kafka Streams) |
| `FAST reconciler down (Kafka Streams)` | `kstreams` profili çalışırken RUNNING instance kalmaması | critical | 2 dakika | Kafka Streams client state |

Önemli noktalar:

- Düşük hacim alert’leri yalnızca İstanbul saatiyle `09:00–18:00` arasında değerlendirilir.
- İlk yedi alert reconciler consumer group’larının (`flink-<flow>-reconciler`) Kafka offset’lerine dayanır. Flink offset’leri checkpoint sırasında commit eder.
- Bir akışa özel eşik gerekirse kuralı kopyalayıp sorguya `{flow="havale"}` gibi bir filtre ekleyin.
- `FAST reconciler down (Kafka Streams)` yalnızca `kstreams` profili çalışıyorsa değerlendirilir.
- Koşul ilk sağlandığında alert `Pending` olur. `for` süresi boyunca devam ederse `Firing` olur.
- Alert’ler Grafana’da görünür; Slack veya e-posta gönderimi için ayrıca contact point ve notification policy tanımlanmalıdır.

## Prometheus metrikleri

Prometheus scrape ayarları [observability/prometheus.yml](./observability/prometheus.yml) dosyasındadır. Genel scrape aralığı 5 saniyedir.

Ana kaynaklar:

| Kaynak | Metrik tipi |
|---|---|
| Producer | `producer_*`, HTTP ve .NET runtime metrikleri |
| Consumer | `consumer_*`, Kafka tüketim ve .NET runtime metrikleri |
| Kafka Streams reconciler | `reconciler_*{impl="kstreams", flow="fast"}`, Kafka Streams ve JVM metrikleri |
| Flink reconciler | Flink metric reporter’ı ve normalize edilmiş `reconciler_*{impl="flink", flow="<flow>"}` metrikleri |
| Kafka exporter | Consumer group lag, offset ve üyelik metrikleri |
| Kafka JMX exporter | Broker ve partition metrikleri |

Flink’in Prometheus reporter’ı MetricGroup boyutlarını metric adının içine taşıdığı için Prometheus scrape konfigürasyonunda normalize edilir. Böylece Flink ve Kafka Streams iş metrikleri aynı sorgu sözleşmesini kullanır. `flow` etiketi scrape sırasında servis adından (`flink-<flow>-taskmanager` / `-jobmanager`) eklenir; Flink’in kendi metrikleri (checkpoint, restart, lag) de bu etiketi taşır.

Prometheus recording rule’ları [observability/prometheus/rules/transfers.yml](./observability/prometheus/rules/transfers.yml) dosyasındadır (`transfer:*`). Consumer group ve akış (`flow`, topic adından) bazında son 5 dakikalık hacim, başarı/başarısızlık oranı ve request/outcome mismatch hesaplarını üretir.

## Kalıcı volume klasörleri

Kalıcı veriler proje altındaki [volumes/](./volumes/) klasöründe tutulur. Bu klasörlerin içeriği `.gitignore` ile Git dışında bırakılır.

| Klasör | Kullanıldığı servis | İçerik |
|---|---|---|
| `volumes/kafka` | Kafka | Kafka log segmentleri ve KRaft metadata |
| `volumes/flink-checkpoints` | Flink servisleri | Flink checkpoint state’i |
| `volumes/prometheus` | Prometheus | Time-series database verisi |
| `volumes/loki` | Loki | Log index ve chunk verisi |
| `volumes/alloy` | Alloy | Alloy agent state’i |
| `volumes/grafana` | Grafana | Grafana database ve state’i |

Producer, consumer, exporter ve load balancer servisleri stateless’tir; bu nedenle onlar için gereksiz kalıcı data volume oluşturulmamıştır. Kafka Streams local RocksDB state’i changelog topic’i ile yeniden oluşturulabildiği için iki replica arasında ortak bir klasör paylaşılmaz.

## Proje yapısı

```text
dotnet/
  Shared/                    Ortak event modelleri ve yardımcıları
  MockProducer/     HTTP API → Kafka producer
  MockConsumer/     Kafka consumer ve teknik metrikler

reconciler/                  Java Kafka Streams reconciler
  src/main/                   Topology, state store ve business metrics
  src/test/                   TopologyTestDriver testleri

flink/reconciler/            Apache Flink reconciler (FAST, Havale, EFT; FLOW ile seçilir)
  src/main/                   Flink job, Flow varsayılanları, process function ve metrics
  src/test/                   Flink reconciliation testleri
  conf/                       Flink runtime config

observability/
  prometheus.yml             Prometheus scrape ayarları
  prometheus/rules/           Recording rules
  grafana/dashboards/         Dashboard JSON dosyaları
  grafana/provisioning/       Datasource, dashboard ve alert provisioning
  loki/                       Loki config
  alloy/                      Alloy config
  kafka-jmx-exporter/         Kafka JMX exporter config
  nginx/                      Producer load balancer config

performance/
  artillery/                  Artillery senaryoları ve raporları
  producer.js                 k6 producer yük testi

volumes/                      Local persistent data; Git tarafından ignore edilir
docker-compose.yml            Local stack tanımı
dashboard.md                 Dashboard ve alert ayrıntıları
```

## Performans testleri

Artillery testleri [performance/artillery/](./performance/artillery/) altındadır. Kurulum:

```powershell
cd performance/artillery
npm install
```

Testler çalışırken ana stack’in hazır olması gerekir. Testler producer endpoint’i üzerinden event gönderir ve raporları `performance/artillery/reports/` altına JSON olarak yazar.

| Komut | Senaryo |
|---|---|
| `npm run test:both-open` | Normal send → success/failed akışı |
| `npm run test:fast-send-backlog` | Outcome olmadan send backlog oluşturma |
| `npm run test:no-fast-send` | Yalnızca outcome event’leri |
| `npm run test:missing-outcomes` | Send/outcome mismatch alarmı |
| `npm run test:high-failure` | Yüksek failed oranı |
| `npm run test:fast-send-spike` | Yüksek trafik |
| `npm run test:low-volume` | Düşük trafik |
| `npm run test:consumer-only` | Consumer `/metrics` endpoint kontrolü |
| `npm run test:all` | Tüm senaryoları sırayla çalıştırır |

Basit k6 testi:

```powershell
k6 run performance/producer.js
$env:PRODUCER_URL = "http://127.0.0.1:8080"
k6 run performance/producer.js
```

Performance testleri sırasında Docker image build, stack yeniden kurma, health check veya servis durumu değiştirme yapılmaz. Test hedeflerinin önceden hazırlanmış olması beklenir; Artillery runner senaryoyu doğrudan çalıştırır. `fast-send-backlog` gibi servis durumu gerektiren senaryolar için servisleri testten önce manuel olarak hazırlayın. Alert senaryolarını aynı anda çalıştırmayın; trafik ve alarm sinyalleri birbirine karışabilir.

## Geliştirme ve test

### .NET

```powershell
dotnet build dotnet/FastTransfer.slnx
dotnet test dotnet/FastTransfer.slnx
```

### Kafka Streams

```powershell
cd reconciler
mvn test
```

### Flink

```powershell
cd flink-fast-reconciler
mvn test
mvn package -DskipTests
```

Java testleri `TopologyTestDriver` ve Flink test harness ile local çalışır; Docker üzerinden test çalıştırmak gerekmez.

## Sorun giderme

### Grafana dashboard görünmüyor

1. Grafana container’ının çalıştığını kontrol edin.
2. Dashboard JSON’unun geçerli JSON olduğunu kontrol edin.
3. Provisioning dosyasını kontrol edin: [dashboards.yml](./observability/grafana/provisioning/dashboards/dashboards.yml).
4. Grafana’yı yeniden başlatın:

```powershell
docker compose restart grafana
```

### Flink job görünmüyor

```powershell
Invoke-RestMethod http://localhost:18082/overview
Invoke-RestMethod http://localhost:18082/jobs/overview
docker compose logs --tail=100 flink-fast-jobmanager
docker compose logs --tail=100 flink-fast-taskmanager
```

Beklenen job adı `flink-fast-reconciler`, durumu `RUNNING` olmalıdır. Havale ve EFT için `18083` / `18084` portlarını ve `flink-havale-*` / `flink-eft-*` servislerini kullanın. Application mode’da job JobManager ile birlikte başlar; JobManager yeniden başlarsa job da yeniden başlar.

### Metrik var ama dashboard boş

Önce Prometheus target durumunu kontrol edin:

```powershell
Invoke-RestMethod http://localhost:9090/api/v1/targets
```

Flink için JobManager ve TaskManager target’larının `up` olması gerekir. Flink iş metrikleri yalnızca ilgili event işlendiğinde oluşabilir; sabit altyapı metrikleri ise job çalışır çalışmaz görünür.

### Kafka output topic’ini okumak

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic fast_transfer_unmatched `
  --from-beginning `
  --consumer-property isolation.level=read_committed
```

## Sürümler

| Bileşen | Sürüm |
|---|---|
| Apache Kafka | 4.4.0 |
| Apache Flink | 2.1.3 |
| Flink Kafka connector | 5.0.0-2.1 |
| Prometheus | 3.13.1 |
| Grafana | 13.3.0 |
| Loki | 3.7.9 |
| Grafana Alloy | 1.21.3 |
| nginx | 1.29.3-alpine |
| kafka-exporter | 1.9.0 |
| .NET | 10 |
| Java | 21 |

Dashboard panel açıklamaları, alert eşikleri ve yeni metrik/panel ekleme rehberi için [dashboard.md](./dashboard.md) dosyasına bakın.
