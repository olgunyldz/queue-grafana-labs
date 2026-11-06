# Dashboard'lar ve alarmlar

Bu dokÃ¼man FAST transfer akÄ±ÅŸÄ±nÄ±n Grafana'da nasÄ±l izlendiÄŸini anlatÄ±r. ÃœÃ§ konuyu kapsar:

1. Verinin Grafana'ya nasÄ±l geldiÄŸi.
2. Her dashboard'un ve panelin ne gÃ¶sterdiÄŸi, neden konulduÄŸu.
3. AlarmlarÄ±n ne zaman, neden Ã§aldÄ±ÄŸÄ± ve Ã§aldÄ±ÄŸÄ±nda nereye bakÄ±lacaÄŸÄ±.

Grafana: http://localhost:3000 (admin/admin). Dashboard'lar `FAST` ve `Kafka` klasÃ¶rlerindedir. Her dashboard'un saÄŸ Ã¼stÃ¼ndeki **FAST lab** menÃ¼sÃ¼ diÄŸerlerine geÃ§iÅŸ saÄŸlar (zaman aralÄ±ÄŸÄ± korunur).

- [1. Veri Grafana'ya nasÄ±l geliyor](#1-veri-grafanaya-nasÄ±l-geliyor)
- [2. Dashboard'lar](#2-dashboardlar)
  - [FAST Operations](#fast-operations)
  - [FAST Services](#fast-services)
  - [Kafka Consumer Groups & Broker](#kafka-consumer-groups--broker)
- [3. Alarmlar](#3-alarmlar)
- [4. Yeni metrik, panel veya alarm eklemek](#4-yeni-metrik-panel-veya-alarm-eklemek)
- [5. Bilinen kÄ±sÄ±tlar](#5-bilinen-kÄ±sÄ±tlar)

---

## 1. Veri Grafana'ya nasÄ±l geliyor

```mermaid
flowchart LR
    subgraph App["Uygulama"]
        P["dotnet-producer Ã—2 (C#)<br/>/metrics :8080"]
        C["dotnet-consumer Ã—2 (C#)<br/>/metrics :2114"]
        R["fast-reconciler Ã—2 (Java, Kafka Streams)<br/>/metrics :9400"]
    end
    K[(Kafka)]
    KE["kafka-exporter<br/>:9308"]
    JMX["kafka-jmx-exporter<br/>:5556"]
    PR[("Prometheus<br/>+ recording rules")]
    AL["Grafana Alloy"]
    LO[("Loki")]
    G["Grafana<br/>dashboard'lar + alarmlar"]

    P -- event --> K -- event --> C
    K -- "event" --> R -- "fast_transfer_matched / unmatched" --> K
    K -- "consumer group offset/lag" --> KE
    K -- JMX --> JMX
    P & C & R & KE & JMX -- scrape 5s --> PR
    P & C & R -. "stdout loglarÄ±" .-> AL --> LO
    PR --> G
    LO --> G
```

Veri altÄ± kaynaktan gelir. Hepsi Prometheus'a (5 saniyede bir scrape) veya Loki'ye gider; Grafana bu ikisini okur:

| Kaynak | Ne Ã¼retir | NasÄ±l toplanÄ±r | KullanÄ±ldÄ±ÄŸÄ± yer |
|---|---|---|---|
| **Producer** (`dotnet-producer`, 2 replica) | YazÄ±lan event sayÄ±sÄ± ve TRY tutarÄ±, HTTP istek sayÄ±sÄ± ve sÃ¼resi, hatalar, .NET runtime | Uygulama `prometheus-net` ile `/metrics` sunar. Prometheus her replica'yÄ± Docker DNS Ã¼zerinden ayrÄ± bulur (`dns_sd_configs`). | FAST Services, FAST Operations (transfer sayÄ±sÄ±) |
| **Consumer** (`dotnet-consumer`, 2 replica) | Teknik metrikler: tÃ¼ketilen event sayÄ±sÄ±, hatalar, Kafka â†’ consumer gecikmesi, partition daÄŸÄ±lÄ±mÄ±, runtime | Producer ile aynÄ± ÅŸekilde: `/metrics`, replica baÅŸÄ±na scrape | FAST Services |
| **Reconciler** (`fast-reconciler`, Kafka Streams, 2 instance) | FAST iÅŸ metrikleri: eÅŸleÅŸen transferler, tamamlanma sÃ¼resi (banka bazÄ±nda), banka/tutar/hata nedeni kÄ±rÄ±lÄ±mlarÄ±, bekleyen/takÄ±lÄ±/eÅŸleÅŸmeyen transferler; Kafka Streams ve JVM metrikleri | Micrometer ile `/metrics` (:9400); Prometheus instance'larÄ± Docker DNS ile bulur. EÅŸleÅŸen ve eÅŸleÅŸmeyen transferler ayrÄ±ca `fast_transfer_matched` / `fast_transfer_unmatched` topic'lerine tek tek yazÄ±lÄ±r. | FAST Operations, FAST Services â†’ Reconciler |
| **kafka-exporter** | Consumer group baÅŸÄ±na committed offset, lag ve Ã¼ye sayÄ±sÄ±; topic'lerin son offset'leri; broker sayÄ±sÄ± | Kafka'ya baÄŸlanÄ±p offset'leri okur, `:9308/metrics` sunar | Alarmlar, Kafka dashboard'u, FAST Operations hacim grafikleri |
| **kafka-jmx-exporter** | Broker iÃ§ metrikleri: mesaj/s, byte/s, controller ve partition saÄŸlÄ±ÄŸÄ±, istek sÃ¼releri | Kafka'nÄ±n JMX portuna (9999) baÄŸlanÄ±r, `:5556/metrics` sunar | Kafka dashboard'u â†’ Broker bÃ¶lÃ¼mÃ¼ |
| **Loglar** | Producer, consumer ve nginx'in stdout loglarÄ± | Grafana Alloy, Docker container'larÄ±nÄ± keÅŸfeder ve loglarÄ± `container` label'Ä±yla Loki'ye gÃ¶nderir | FAST Services â†’ Logs |

### Hangi bilgi nereden gelir

AynÄ± akÄ±ÅŸ Ã¼Ã§ farklÄ± kaynaktan Ã¶lÃ§Ã¼lÃ¼r. Her birinin gÃ¼Ã§lÃ¼ olduÄŸu yer farklÄ±dÄ±r:

- **Reconciler metrikleri** (`fast_kstreams_reconciler_*`): FAST'in **iÅŸ** gÃ¶rÃ¼nÃ¼mÃ¼.
  - Her transfer `fast_send` ile sonucunun eÅŸleÅŸmesinden sonra **bir kez** sayÄ±lÄ±r. Banka ve tutar her zaman `fast_send`'den alÄ±nÄ±r.
  - Tamamlanma sÃ¼resi, takÄ±lÄ± ve eÅŸleÅŸmeyen transferler yalnÄ±zca burada vardÄ±r, Ã§Ã¼nkÃ¼ iki event'i birleÅŸtirmek durum gerektirir.
  - Durum Kafka Streams state store'undadÄ±r. Restart, Ã¶lÃ§ekleme veya rebalance'ta kaybolmaz.
- **Uygulama sayaÃ§larÄ±** (`producer_*`, `consumer_*`): servislerin **teknik** saÄŸlÄ±ÄŸÄ±.
  - API istekleri, hatalar, tÃ¼ketim hÄ±zÄ±, replica daÄŸÄ±lÄ±mÄ± ve gecikme.
  - Process yeniden baÅŸladÄ±ÄŸÄ±nda **sÄ±fÄ±rlanÄ±r**. Panellerde bu yÃ¼zden `rate()` / `increase()` ile kullanÄ±lÄ±r.
- **Kafka offset'leri** (`kafka_consumergroup_*`, `kafka_topic_partition_*`): consumer group bazÄ±nda ve **kalÄ±cÄ±**.
  - Hacim alarmlarÄ± bilinÃ§li olarak buna dayanÄ±r, bÃ¶ylece reconciler Ã§alÄ±ÅŸmasa da hacim alarmlarÄ± Ã§alÄ±ÅŸÄ±r.
  - Lag ve partition panelleri de buradan gelir.

**Ã‡Ä±ktÄ± topic'lerinin offset'leri sayÄ±m iÃ§in kullanÄ±lmaz.** `fast_transfer_matched` / `fast_transfer_unmatched` exactly-once ile yazÄ±lÄ±r; offset'ler transaction iÅŸaretlerini de iÃ§erdiÄŸi iÃ§in kayÄ±t sayÄ±sÄ±ndan bÃ¼yÃ¼ktÃ¼r. SayÄ±lar reconciler metriklerinden, tek tek kayÄ±tlar topic'lerden okunur.

### Prometheus recording rule'larÄ±

Alarmlar ve bazÄ± paneller, [observability/prometheus/rules/fast.yml](./observability/prometheus/rules/fast.yml) iÃ§indeki Ã¶nceden hesaplanmÄ±ÅŸ serileri kullanÄ±r. Kurallar 15 saniyede bir hesaplanÄ±r ve tÃ¼mÃ¼ `consumergroup` bazÄ±ndadÄ±r:

| Seri | AnlamÄ± |
|---|---|
| `fast:sent:increase5m` | Group'un son 5 dakikada tÃ¼kettiÄŸi `fast_send` sayÄ±sÄ± |
| `fast:outcome:increase5m` | Son 5 dakikada tÃ¼ketilen `fast_send_success + fast_send_failed` |
| `fast:success:increase5m`, `fast:failed:increase5m` | SonuÃ§larÄ±n ayrÄ± ayrÄ± sayÄ±sÄ± |
| `fast:failed_success:ratio5m` | `failed / success`. 5 dakikada 20'den az sonuÃ§ varsa hesaplanmaz. |
| `fast:outcome_mismatch:ratio5m` | `\|send âˆ’ outcome\| / send`. 5 dakikada 20'den az event varsa hesaplanmaz. |
| `fast:business_hours` | Ä°stanbul saatiyle 09:00â€“18:00 arasÄ± `1`, diÄŸer saatlerde `0` |

Bu seriler iki iÅŸe yarar:

- **Tek tanÄ±m:** alarm ve dashboard aynÄ± tanÄ±mÄ± kullanÄ±r, yani dashboard'da gÃ¶rÃ¼len Ã§izgi alarmÄ±n baktÄ±ÄŸÄ± deÄŸerin kendisidir.
- **HÄ±z:** sorgular hafifler.

### Grafana'ya provisioning

Grafana elle yapÄ±landÄ±rÄ±lmaz; her ÅŸey dosyadan yÃ¼klenir (`observability/grafana/provisioning/`):

| Dosya | Ne yÃ¼kler |
|---|---|
| `datasources/prometheus.yml`, `datasources/loki.yml` | Prometheus ve Loki veri kaynaklarÄ±. UID'ler sabittir, alarmlar bu UID'lere baÄŸlÄ±dÄ±r. |
| `dashboards/dashboards.yml` | `observability/grafana/dashboards/` altÄ±ndaki JSON'lar. **Alt klasÃ¶r adÄ± Grafana klasÃ¶rÃ¼ olur** (`FAST/`, `Kafka/`). |
| `alerting/fast-alerts.yml` | FAST alarmlarÄ±, `FAST` klasÃ¶rÃ¼ne yÃ¼klenir |

---

## 2. Dashboard'lar

| KlasÃ¶r | Dashboard | Kimin iÃ§in | CevapladÄ±ÄŸÄ± soru |
|---|---|---|---|
| FAST | [FAST Operations](#fast-operations) | Ä°ÅŸ / operasyon | Transferler saÄŸlÄ±klÄ± mÄ±? Ne kadar para geÃ§ti, hangi banka sorunlu? |
| FAST | [FAST Services](#fast-services) | Uygulama ekibi | Servisler ve replica'lar saÄŸlÄ±klÄ± mÄ±? Sorun API'de mi, consumer'da mÄ±? |
| Kafka | [Kafka Consumer Groups & Broker](#kafka-consumer-groups--broker) | Platform | Kafka geride kalÄ±yor mu? Hangi partition birikiyor, broker saÄŸlÄ±klÄ± mÄ±? |

Neden Ã¼Ã§ ayrÄ± dashboard var:

- Her biri farklÄ± bir kitleye farklÄ± bir soruyu cevaplar.
- Tek dashboard'da toplandÄ±ÄŸÄ±nda (Ã¶nceki sÃ¼rÃ¼mde 90+ panel vardÄ±) aynÄ± bilgi birkaÃ§ yerde tekrar ediyor ve Ã¶nemli sinyaller kayboluyordu.
- Bir sorun Ã¶nce **FAST Operations**'ta gÃ¶rÃ¼lÃ¼r. KÃ¶k neden **FAST Services**'te (uygulama) veya **Kafka** dashboard'unda (altyapÄ±) aranÄ±r.

### FAST Operations

`FAST / FAST Operations` (`/d/fast-operations`): iÅŸ gÃ¶zÃ¼yle FAST akÄ±ÅŸÄ±.

**DeÄŸiÅŸkenler:**

- `Consumer group`: hacim ve oran grafikleri iÃ§in (varsayÄ±lan `fast-transfer-consumers-dotnet`).
- `Bank role`: `sender` veya `receiver`.
- `Bank`: banka filtresi.

**Ä°ÅŸaret:** consumer group Ã¼yeliÄŸi deÄŸiÅŸtiÄŸinde (restart/rebalance) grafiklerde mor iÅŸaret Ã§Ä±kar.

#### Overview

SayfayÄ± aÃ§an kiÅŸinin 5 saniyede durumu anlamasÄ± iÃ§in var. Tutar ve sayÄ±lar seÃ§ili zaman aralÄ±ÄŸÄ± iÃ§indir.

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Transfers (fast_send) | Kafka'ya yazÄ±lan `fast_send` sayÄ±sÄ± (topic offset'leri) | TrafiÄŸin bÃ¼yÃ¼klÃ¼ÄŸÃ¼ |
| Success rate | EÅŸleÅŸen transferlerde `success / (success + failed)` | En temel iÅŸ saÄŸlÄ±ÄŸÄ± gÃ¶stergesi; %95 altÄ± sarÄ±, %80 altÄ± kÄ±rmÄ±zÄ± |
| Completion time p95 | `fast_send` â†’ sonuÃ§ sÃ¼resi | FAST "anlÄ±k" olmalÄ±; bu deÄŸer fiilen SLA'dÄ±r |
| Stuck transfers (> 30s) | 30 sn'den uzun sonuÃ§ bekleyen transfer | MÃ¼ÅŸterinin "param nerede" dediÄŸi transferler |
| Successful volume | BaÅŸarÄ±lÄ± transferlerin TRY toplamÄ± | Ä°ÅŸin parasal bÃ¼yÃ¼klÃ¼ÄŸÃ¼ |
| Failed amount (at risk) | BaÅŸarÄ±sÄ±z transferlerin TRY toplamÄ± | Hata sayÄ±sÄ±ndan daha anlamlÄ± bir risk gÃ¶stergesi |

#### Alerts

AlarmlarÄ±n gÃ¶rÃ¼nÃ¼r olmasÄ± iÃ§in bu bÃ¶lÃ¼m sayfanÄ±n en Ã¼stÃ¼ndedir.

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Firing / Pending | Ã‡alan ve beklemedeki FAST alarmÄ± sayÄ±sÄ± (kÄ±rmÄ±zÄ± / turuncu) | Tek bakÄ±ÅŸta "bir ÅŸey var mÄ±?" sorusunun cevabÄ± |
| FAST alerts | 9 kuralÄ±n tamamÄ± ve durumlarÄ±; Ã§alanlar en Ã¼stte | Hangi alarmÄ±n, hangi consumer group iÃ§in Ã§aldÄ±ÄŸÄ± |
| Thresholds | Her alarmÄ±n eÅŸiÄŸi | EÅŸikleri aramadan gÃ¶rmek iÃ§in |

#### Volume vs alert thresholds

AlarmlarÄ±n baktÄ±ÄŸÄ± deÄŸerlerin zaman iÃ§indeki hÃ¢li. Alarm eÅŸikleri grafiklerde **kesikli Ã§izgi** olarak Ã§izilir; bir alarmÄ±n neden Ã§aldÄ±ÄŸÄ± (veya Ã§almadÄ±ÄŸÄ±) burada gÃ¶rÃ¼lÃ¼r.

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| fast_send per 5m | Son 5 dakikada tÃ¼ketilen `fast_send`; geÃ§en haftanÄ±n aynÄ± saati (gri kesikli); mesai saatleri (gÃ¶lge) | DÃ¼ÅŸÃ¼k / yÃ¼ksek hacim alarmlarÄ±. GeÃ§en hafta Ã§izgisi "bu saat iÃ§in normal ne" sorusunu cevaplar; eÅŸikleri ayarlamaya yarar. |
| Outcomes (success + failed) per 5m | AynÄ±sÄ±, sonuÃ§ event'leri iÃ§in | SonuÃ§ hacmi alarmlarÄ± |
| fast_send_failed per 5m | 5 dakikalÄ±k baÅŸarÄ±sÄ±z sayÄ±sÄ±; Ã§izgi 500'de | `FAST failed count high` |
| failed / success ratio | BaÅŸarÄ±sÄ±z / baÅŸarÄ±lÄ± oranÄ±; Ã§izgi %20'de | `FAST failed/success ratio high` |
| send / outcome mismatch | Sonucu gelmeyen (veya send'i olmayan) transfer oranÄ±; Ã§izgi %10'da | `FAST send/outcome mismatch` |

#### Transfer completion

Reconciler, her `fast_send`'i aynÄ± `transaction_id`'li sonucuyla eÅŸleÅŸtirir. Bu bÃ¶lÃ¼m o eÅŸleÅŸtirmenin sonucudur.

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Completion time p50 / p95 / p99 | Tamamlanma sÃ¼resi yÃ¼zdelikleri | YavaÅŸlamayÄ± erken gÃ¶rmek; p99 kuyruktaki uÃ§ durumlarÄ± gÃ¶sterir |
| Completion time distribution | SÃ¼relerin heatmap'i | OrtalamanÄ±n gizlediÄŸi iki tepeli daÄŸÄ±lÄ±mlarÄ± gÃ¶rmek |
| Slowest banks: completion p95 (top 5) | GÃ¶nderen banka baÅŸÄ±na tamamlanma sÃ¼resi | "Hangi banka yavaÅŸ?" sorusunun cevabÄ±. Bu panel yalnÄ±zca eÅŸleÅŸtirmeyle mÃ¼mkÃ¼ndÃ¼r. |
| Waiting and unmatched transfers | SonuÃ§ bekleyen, 30 sn'den uzun bekleyen (takÄ±lÄ±), send'i olmayan sonuÃ§lar ve 10 dakikada eÅŸleÅŸmeyenler (tÃ¼rÃ¼ne gÃ¶re) | "KayÄ±p" transferleri **tek tek** sayar. Mismatch alarmÄ± toplam sayÄ±larÄ± karÅŸÄ±laÅŸtÄ±rÄ±rken bu panel eÅŸleÅŸmeye bakar. EÅŸleÅŸmeyenlerin `transaction_id`'leri `fast_transfer_unmatched` topic'indedir. |

#### Banks ($role)

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Bank summary | Banka baÅŸÄ±na tamamlanan transfer, baÅŸarÄ±lÄ±, baÅŸarÄ±sÄ±z, hata oranÄ±, baÅŸarÄ±lÄ± TRY hacmi | "Sorun hangi bankada?" sorusunun cevabÄ±. Hata oranÄ± hÃ¼cresi %10 Ã¼stÃ¼nde sarÄ±, %20 Ã¼stÃ¼nde kÄ±rmÄ±zÄ±dÄ±r. Banka ve tutar `fast_send`'den alÄ±ndÄ±ÄŸÄ± iÃ§in sonuÃ§ event'leri bu bilgiyi taÅŸÄ±masa da doÄŸrudur. |
| Successful volume share (TRY) | BankalarÄ±n hacim payÄ± | Bir bankadaki sorunun ne kadar parayÄ± etkilediÄŸi |
| Failure rate by bank (top 5) | En kÃ¶tÃ¼ 5 bankanÄ±n hata oranÄ± | Sorunun ne zaman baÅŸladÄ±ÄŸÄ±nÄ± ve hangi bankada olduÄŸunu gÃ¶rmek |
| Completed transfers / s by bank | Banka baÅŸÄ±na tamamlanan transfer hÄ±zÄ± | Bir bankanÄ±n trafiÄŸinin aniden dÃ¼ÅŸmesi, o tarafta bir kesinti iÅŸareti olabilir |

`Bank role` = `sender` gÃ¶nderen bankaya, `receiver` alÄ±cÄ± bankaya gÃ¶re kÄ±rar. Enpara.com, QNB'nin kodunu (00111) kullandÄ±ÄŸÄ± iÃ§in `QNB / Enpara.com` olarak birlikte gÃ¶rÃ¼nÃ¼r. Listede olmayan kodlar `other` altÄ±nda toplanÄ±r.

#### Money and failures

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| TRY volume per minute | DakikalÄ±k TRY hacmi (baÅŸarÄ±lÄ± / baÅŸarÄ±sÄ±z) | Parasal akÄ±ÅŸÄ±n zaman iÃ§indeki hÃ¢li |
| Average transfer amount | Ortalama tutar | OlaÄŸan dÄ±ÅŸÄ± tutar desenlerini (Ã¶r. ani bÃ¼yÃ¼k transferler) fark etmek |
| Amount distribution | Tutar aralÄ±klarÄ±na gÃ¶re transfer sayÄ±sÄ± | TrafiÄŸin hangi tutar bandÄ±nda yoÄŸunlaÅŸtÄ±ÄŸÄ± |
| Failed transfers by reason | Hata nedenleri (`INSUFFICIENT_BALANCE`, `RECEIVER_BANK_TIMEOUT`â€¦) | HatanÄ±n mÃ¼ÅŸteri kaynaklÄ± mÄ±, sistem kaynaklÄ± mÄ± olduÄŸunu ayÄ±rmak |

### FAST Services

`FAST / FAST Services` (`/d/fast-services`): servislerin ve replica'larÄ±n teknik saÄŸlÄ±ÄŸÄ±.

**DeÄŸiÅŸkenler:**

- `Consumer group`
- `Service`: log paneli iÃ§in.
- `Log filter`: regex.

#### Replicas

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Producer / Consumer replicas up | Ayakta olan replica sayÄ±sÄ± (2 = yeÅŸil) | Ã–lÃ§eklenmiÅŸ sistemde bir replica'nÄ±n dÃ¼ÅŸmesi trafikte hemen gÃ¶rÃ¼nmez; burada gÃ¶rÃ¼nÃ¼r |
| Consumer group members | Kafka'nÄ±n gÃ¶rdÃ¼ÄŸÃ¼ Ã¼ye sayÄ±sÄ± | Ayakta olan consumer replica sayÄ±sÄ±na eÅŸit olmalÄ±; farklÄ±ysa replica Ã§alÄ±ÅŸÄ±yor ama group'a katÄ±lamamÄ±ÅŸtÄ±r |
| API error rate / API p95 | 5 dakikalÄ±k 2xx dÄ±ÅŸÄ± oran ve yanÄ±t sÃ¼resi | API saÄŸlÄ±ÄŸÄ±nÄ±n Ã¶zeti |
| Consumer errors (5m) | Okuma veya decode hatalarÄ± | Consumer saÄŸlÄ±ÄŸÄ±nÄ±n Ã¶zeti |

#### Producer API

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Requests / s by endpoint and status | Endpoint ve HTTP kodu bazÄ±nda istek hÄ±zÄ± (4xx turuncu, 5xx kÄ±rmÄ±zÄ±) | Hangi event'te sorun olduÄŸu |
| Response time p95 / p99 | Endpoint bazÄ±nda yanÄ±t sÃ¼resi (Kafka'ya yazma dahil) | Kafka yavaÅŸladÄ±ÄŸÄ±nda ilk burada gÃ¶rÃ¼lÃ¼r |
| Requests / s per replica | Replica baÅŸÄ±na istek | nginx yÃ¼kÃ¼nÃ¼ dengeli daÄŸÄ±tÄ±yor mu? |
| Error rate: 4xx vs 5xx | DoÄŸrulama hatalarÄ± ile Kafka yazma hatalarÄ±nÄ± ayÄ±rÄ±r | 4xx istemci sorunudur, 5xx bizim sorunumuzdur |
| Producer errors by reason | `invalid_json` / `validation` / `kafka` | HatanÄ±n kÃ¶k nedeni |

#### Consumer replicas

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Events consumed / s per replica | Replica baÅŸÄ±na tÃ¼ketim | Ä°ÅŸ yÃ¼kÃ¼ daÄŸÄ±lÄ±mÄ±. 3 partition 2 replica'ya daÄŸÄ±ldÄ±ÄŸÄ± iÃ§in oran yaklaÅŸÄ±k 2:1'dir. |
| Partitions per replica | Hangi replica'nÄ±n hangi partition'Ä± okuduÄŸu | Rebalance sonrasÄ± daÄŸÄ±lÄ±mÄ±n deÄŸiÅŸtiÄŸini gÃ¶rmek |
| Kafka â†’ consumer latency p95 | MesajÄ±n Kafka'ya yazÄ±lmasÄ±ndan tÃ¼ketilmesine kadar geÃ§en sÃ¼re | Backlog varken yÃ¼kselir; hangi replica'nÄ±n geride kaldÄ±ÄŸÄ±nÄ± gÃ¶sterir |
| Consumer errors / s | Replica ve neden bazÄ±nda hatalar | Sorunun yerini bulmak |

#### Reconciler (Kafka Streams)

EÅŸleÅŸtirmeyi yapan servisin saÄŸlÄ±ÄŸÄ±. Bu servis Ã§alÄ±ÅŸmazsa FAST Operations'taki tamamlanma, banka ve tutar panelleri gÃ¼ncellenmez. Hacim alarmlarÄ± ise Kafka offset'lerine dayandÄ±ÄŸÄ± iÃ§in Ã§alÄ±ÅŸmaya devam eder.

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Instances RUNNING | Kafka Streams durumu `RUNNING` olan instance sayÄ±sÄ± | `FAST reconciler down` alarmÄ±nÄ±n baktÄ±ÄŸÄ± deÄŸer |
| Input lag | Reconciler'Ä±n henÃ¼z okumadÄ±ÄŸÄ± FAST event'leri | Reconciler geride kalÄ±rsa iÅŸ metrikleri gecikir |
| Restore remaining | Rebalance sonrasÄ± changelog'dan geri yÃ¼klenecek kayÄ±t | Ã–lÃ§ekleme veya restart sonrasÄ± state'in taÅŸÄ±nmasÄ±nÄ± izlemek; 0'a inmeli |
| Failed stream threads | Ã‡Ã¶ken Kafka Streams thread'leri | Thread'ler otomatik yenilenir; artÄ±ÅŸ bir hatanÄ±n tekrarladÄ±ÄŸÄ±nÄ± gÃ¶sterir |
| In-flight transfers / Stuck | State store'da diÄŸer yarÄ±sÄ±nÄ± bekleyen kayÄ±tlar | EÅŸleÅŸtirme yÃ¼kÃ¼ |
| Records processed / s per instance | Instance baÅŸÄ±na iÅŸlenen kayÄ±t | YÃ¼kÃ¼n instance'lara daÄŸÄ±lÄ±mÄ± |
| Reconciler lag by topic | Girdi topic'leri ve iÃ§ repartition topic'i iÃ§in lag | Hangi aÅŸamanÄ±n geride kaldÄ±ÄŸÄ± |
| Commit / scan latency | Exactly-once transaction commit sÃ¼resi ve periyodik state taramasÄ±nÄ±n sÃ¼resi | YavaÅŸlama kaynaÄŸÄ±nÄ± bulmak; tarama sÃ¼resi state bÃ¼yÃ¼dÃ¼kÃ§e artar |
| In-flight transfers per instance | Instance baÅŸÄ±na bekleyen kayÄ±tlar | Ã–lÃ§ekleme sÄ±rasÄ±nda state'in instance'lar arasÄ±nda taÅŸÄ±ndÄ±ÄŸÄ±nÄ± gÃ¶rmek |
| JVM heap per instance | Java heap kullanÄ±mÄ± | Bellek sÄ±zÄ±ntÄ±sÄ± veya kapasite |

#### Runtime (per replica)

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Managed heap / Working set | .NET GC heap ve process belleÄŸi | Bellek sÄ±zÄ±ntÄ±sÄ± (sÃ¼rekli artan Ã§izgi) |
| CPU | Replica baÅŸÄ±na CPU (1.0 = bir Ã§ekirdek) | Ã–lÃ§ekleme ihtiyacÄ± |
| GC collections / s | GC sÄ±klÄ±ÄŸÄ± | YÃ¼k altÄ±nda GC baskÄ±sÄ± |

#### Logs

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Error / warning log lines per minute | Producer, consumer ve reconciler container'larÄ± iÃ§in hata/uyarÄ± log sayÄ±sÄ± (Loki) | Metriklerin yakalamadÄ±ÄŸÄ± hatalarÄ± gÃ¶rmek |
| Error / warning logs | Hata ve uyarÄ± satÄ±rlarÄ± | Bir grafikteki sÄ±Ã§ramanÄ±n ayrÄ±ntÄ±sÄ±na inmek |
| All service logs | `Service` ve `Log filter` ile filtrelenen tÃ¼m loglar | Belirli bir transferi izlemek, Ã¶r. filtreye bir `transaction_id` yazarak |

### Kafka Consumer Groups & Broker

`Kafka / Kafka Consumer Groups & Broker` (`/d/kafka-consumer-groups`): Kafka tarafÄ±nÄ±n teknik saÄŸlÄ±ÄŸÄ±.

**DeÄŸiÅŸkenler:**

- `Consumer group`: Ã§oklu seÃ§im yapÄ±labilir.
- `Topic`: yalnÄ±zca `fast_send*` topic'leri listelenir.

#### Consumer group

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Group members | Group'taki consumer sayÄ±sÄ± | 0 ise kimse tÃ¼ketmiyordur |
| Total lag | YazÄ±lÄ±p henÃ¼z tÃ¼ketilmemiÅŸ mesaj sayÄ±sÄ± | Consumer'Ä±n geride kalÄ±p kalmadÄ±ÄŸÄ±nÄ±n ana gÃ¶stergesi |
| Consume rate / Produce rate | TÃ¼ketme ve Ã¼retme hÄ±zÄ± | TÃ¼ketme, Ã¼retmeden sÃ¼rekli dÃ¼ÅŸÃ¼kse lag bÃ¼yÃ¼r |
| Estimated drain time | `lag / tÃ¼ketme hÄ±zÄ±` | BirikmiÅŸ iÅŸin ne zaman biteceÄŸi |
| Topic partitions | Toplam partition sayÄ±sÄ± | Consumer Ã¶lÃ§eklemesinin Ã¼st sÄ±nÄ±rÄ±: partition sayÄ±sÄ±ndan fazla replica boÅŸta kalÄ±r |
| Produce vs consume rate | Ä°ki hÄ±zÄ±n zaman iÃ§indeki hÃ¢li | Lag'in neden bÃ¼yÃ¼dÃ¼ÄŸÃ¼nÃ¼ gÃ¶rmek |
| Lag by topic | Topic baÅŸÄ±na lag | `fast-send-backlog` testinde yalnÄ±zca `fast_send` lag'i bÃ¼yÃ¼r |

#### Partitions

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Lag per partition | Topic ve partition baÅŸÄ±na lag | Lag tek bir partition'daysa sorun o partition'Ä±n sahibi olan replica'dadÄ±r |
| Consume rate per partition | Partition baÅŸÄ±na tÃ¼ketim hÄ±zÄ± | DurmuÅŸ bir partition'Ä± bulmak |
| Group members over time | Ãœye sayÄ±sÄ±nÄ±n deÄŸiÅŸimi | Restart ve rebalance anlarÄ±nÄ± gÃ¶rmek |
| Partition status | End offset, committed offset ve lag tablosu | Tam sayÄ±larla doÄŸrulama |
| Lag by topic / partition | Lag'in Ã§ubuk grafiÄŸi | En Ã§ok biriken partition'Ä± tek bakÄ±ÅŸta gÃ¶rmek |

#### Broker

| Panel | Ne gÃ¶sterir | Neden var |
|---|---|---|
| Brokers / Active controller | Ayakta olan broker ve controller | Kafka ayakta mÄ±? |
| Offline / Under-replicated partitions | EriÅŸilemeyen veya kopyasÄ± eksik partition | 0 olmalÄ±; deÄŸilse veri riski vardÄ±r |
| Messages in / s by topic | Broker'a gelen mesaj hÄ±zÄ± | Producer metriklerini Kafka tarafÄ±ndan doÄŸrulamak |
| Bytes in / out | Broker trafiÄŸi | AÄŸ ve disk kapasitesi |
| Request time p99 | Produce ve FetchConsumer isteklerinin broker tarafÄ±ndaki sÃ¼resi | API yavaÅŸsa sorunun Kafka'da olup olmadÄ±ÄŸÄ±nÄ± ayÄ±rmak. FetchConsumer, boÅŸta beklerken ~500 ms gÃ¶sterir; bu normaldir. |

---

## 3. Alarmlar

Alarmlar Grafana Alerting ile tanÄ±mlÄ±dÄ±r ([fast-alerts.yml](./observability/grafana/provisioning/alerting/fast-alerts.yml)) ve `FAST` klasÃ¶rÃ¼ndedir.

- **Ä°lk yedi alarm:** `fast-transfer-consumers-dotnet` consumer group'unun Kafka offset'lerine bakar.
- **Son iki alarm:** reconciler'Ä±n kendi metriklerine bakar.

**NasÄ±l deÄŸerlendirilir:**

- **DeÄŸerlendirme sÄ±klÄ±ÄŸÄ±:** 30 saniyede bir.
- **SayÄ±lan deÄŸer (ilk yedi alarm):** group'un **son 5 dakikada** tÃ¼kettiÄŸi event'ler. Kaynak Kafka offset'leridir (yukarÄ±daki recording rule'lar).
- **Bekleme sÃ¼resi:** koÅŸul ilk saÄŸlandÄ±ÄŸÄ±nda alarm **pending** olur. KoÅŸul **5 dakika boyunca** sÃ¼rerse **firing** olur. KÄ±sa sÄ±Ã§ramalar alarm Ã¼retmez.
- **Veri yokken** (dÃ¼ÅŸÃ¼k trafik veya mesai dÄ±ÅŸÄ±): alarm **Normal** kalÄ±r.

| Alarm | KoÅŸul | Ne zaman | Severity | Neden var | Ã‡aldÄ±ÄŸÄ±nda nereye bakÄ±lÄ±r |
|---|---|---|---|---|---|
| **FAST send volume low (business hours)** | `fast_send` < 100 / 5 dk | 09:00â€“18:00 Ä°stanbul | warning | Mesai saatinde trafiÄŸin dÃ¼ÅŸmesi; genelde istemci, aÄŸ veya producer tarafÄ±nda bir kesinti | FAST Services â†’ Producer API ve Replicas; producer loglarÄ± |
| **FAST send volume high** | `fast_send` > 6000 / 5 dk | Her zaman | critical | Beklenmeyen yÃ¼k: kampanya, tekrar eden istekler veya kÃ¶tÃ¼ye kullanÄ±m | FAST Services â†’ Requests by endpoint; Kafka â†’ lag |
| **FAST outcome volume low (business hours)** | `success + failed` < 100 / 5 dk | 09:00â€“18:00 Ä°stanbul | warning | Transferler gidiyor ama sonuÃ§lar gelmiyor | FAST Operations â†’ Waiting transfers; Kafka â†’ Lag by topic |
| **FAST outcome volume high** | `success + failed` > 6000 / 5 dk | Her zaman | critical | SonuÃ§ akÄ±ÅŸÄ±nda olaÄŸan dÄ±ÅŸÄ± yÃ¼k (Ã¶r. bir backlog'un bir anda boÅŸalmasÄ±) | Kafka â†’ Produce vs consume rate |
| **FAST failed count high** | `fast_send_failed` > 500 / 5 dk | Her zaman | critical | Mutlak hata sayÄ±sÄ± yÃ¼ksek | FAST Operations â†’ Failed by reason ve Bank summary |
| **FAST failed/success ratio high** | `failed / success` > %20 | Her zaman | critical | Trafik dÃ¼ÅŸÃ¼kken de anlamlÄ± olan oransal bozulma | FAST Operations â†’ Failure rate by bank (top 5) |
| **FAST send/outcome mismatch** | `\|send âˆ’ outcome\| / send` > %10 | Her zaman | warning | Sonucu gelmeyen transferler veya send'i olmayan sonuÃ§lar (veri kaybÄ±, eksik entegrasyon) | FAST Operations â†’ Waiting and unmatched transfers; Kafka â†’ Lag by topic |
| **FAST stuck transfers high** | 30 sn'den uzun sonuÃ§ bekleyen transfer > 50 | Her zaman | warning | MÃ¼ÅŸterinin parasÄ± Ã§Ä±kmÄ±ÅŸ ama sonuÃ§ gelmemiÅŸ; karÅŸÄ± banka veya sonuÃ§ akÄ±ÅŸÄ±nda bir sorun | FAST Operations â†’ Slowest banks ve Waiting and unmatched; `fast_transfer_unmatched` topic'i |
| **FAST reconciler down** | `RUNNING` reconciler instance'Ä± yok (2 dk) | Her zaman | critical | Ä°ÅŸ metrikleri (tamamlanma, banka, tutar, takÄ±lÄ±) gÃ¼ncellenmiyor | FAST Services â†’ Reconciler; `docker compose logs fast-reconciler` |

**Notlar:**

- **Mesai saati koÅŸulu:** dÃ¼ÅŸÃ¼k hacim alarmlarÄ± yalnÄ±zca mesai saatlerinde deÄŸerlendirilir. Gece trafiÄŸin dÃ¼ÅŸÃ¼k olmasÄ± normaldir ve alarm Ã¼retmemelidir.
- **Oranlar iÃ§in en az 20 event:** 5 dakikada 20'nin altÄ±nda event varsa oranlar hesaplanmaz. Aksi hÃ¢lde dÃ¼ÅŸÃ¼k trafikte tek bir sonuÃ§suz transfer "%100 mismatch" yapardÄ±.
- **Consumer tamamen durursa:** mesai saatlerinde **dÃ¼ÅŸÃ¼k hacim** alarmlarÄ± Ã§alar, Ã§Ã¼nkÃ¼ group hiÃ§bir ÅŸey tÃ¼ketmez. Mesai dÄ±ÅŸÄ±nda bu durum Kafka dashboard'undaki lag ile gÃ¶rÃ¼lÃ¼r.
- **Backlog boÅŸalÄ±rken:** bir `fast_send` backlog'u bir anda tÃ¼ketildiÄŸinde, kÄ±sa sÃ¼reliÄŸine `mismatch` alarmÄ± **pending** olabilir. Backlog 5 dakikadan kÄ±sa sÃ¼rede eriyorsa firing olmaz.
- **Reconciler alarmÄ±nÄ±n veri yokken davranÄ±ÅŸÄ±:** `FAST reconciler down` iÃ§in "veri yok" durumu da alarm sayÄ±lÄ±r (`noDataState: Alerting`). HiÃ§bir instance scrape edilemiyorsa da Ã§alar.
- **Bildirim yok:** henÃ¼z bir contact point tanÄ±mlÄ± deÄŸil. Alarmlar yalnÄ±zca Grafana'da (Alerting menÃ¼sÃ¼ ve FAST Operations â†’ Alerts) gÃ¶rÃ¼nÃ¼r. Slack veya e-posta iÃ§in Grafana'da bir contact point ve notification policy eklenmelidir.

**AlarmlarÄ± test etmek** iÃ§in Artillery case'leri vardÄ±r ([performance/artillery/README.md](./performance/artillery/README.md)). Alarm testleri aynÄ± anda Ã§alÄ±ÅŸtÄ±rÄ±lmamalÄ±dÄ±r, Ã§Ã¼nkÃ¼ trafikleri birbirine karÄ±ÅŸÄ±r.

| Case | TetiklediÄŸi alarm |
|---|---|
| `missing-outcomes` | send/outcome mismatch |
| `high-failure` | failed count high, failed/success ratio high |
| `fast-send-spike` | send volume high, outcome volume high |
| `low-volume` (yalnÄ±zca mesai saatinde) | send volume low, outcome volume low |
| `fast-send-backlog` (consumer kapalÄ±, sonuÃ§ yok) | 30 sn sonra takÄ±lÄ± transferler artar; 50'yi geÃ§ip 5 dk sÃ¼rerse stuck transfers high |
| `docker compose stop fast-reconciler` (elle) | 2 dk sonra reconciler down |

**EÅŸik veya sÃ¼re deÄŸiÅŸtirmek:**

1. `fast-alerts.yml` iÃ§inde ilgili kuralÄ±n `params` (eÅŸik) ve `for` (sÃ¼re) deÄŸerini deÄŸiÅŸtirin.
2. DosyanÄ±n baÅŸÄ±ndaki tabloyu gÃ¼ncelleyin.
3. `docker compose restart grafana` Ã§alÄ±ÅŸtÄ±rÄ±n.

Mesai saatleri ise `rules/fast.yml` iÃ§indeki `fast:business_hours` kuralÄ±ndadÄ±r (`9` / `18`, UTC+3 sabit). DeÄŸiÅŸtirdikten sonra `docker compose kill -s HUP prometheus` Ã§alÄ±ÅŸtÄ±rÄ±n.

---

## 4. Yeni metrik, panel veya alarm eklemek

Bir bilginin dashboard'da gÃ¶rÃ¼nmesi iÃ§in zincirin her halkasÄ± tamamlanmalÄ±dÄ±r. Kaynaktan Grafana'ya doÄŸru:

### a) Uygulamada metrik Ã¼retmek

Ã–nce metriÄŸin nereye ait olduÄŸuna karar verin:

- **Tek bir event'ten Ã§Ä±kan teknik bir Ã¶lÃ§Ã¼m** (istek sÃ¼resi, hata, tÃ¼ketim hÄ±zÄ±): producer veya consumer'a, `prometheus-net` ile.
- **Transfer bazÄ±nda bir iÅŸ Ã¶lÃ§Ã¼mÃ¼** (iki event'in birleÅŸimi, banka/tutar kÄ±rÄ±lÄ±mÄ±, sÃ¼re): reconciler'a, Micrometer ile ([ReconcilerMetrics.java](./reconciler/src/main/java/fast/reconciler/ReconcilerMetrics.java)):

  ```java
  Counter.builder("fast.kstreams.reconciler.refunds")          // Prometheus: fast_kstreams_reconciler_refunds_total
          .tags("reason", reason).register(registry).increment();
  ```

C# servislerinde Ã¶r. [ConsumerApp.cs](./dotnet/FastTransfer.Consumer/ConsumerApp.cs):

```csharp
static readonly Counter Refunds = Metrics.CreateCounter(
    "consumer_fast_refunds_total", "Refunded FAST transfers, by reason.", "reason");
...
Refunds.WithLabels(reason).Inc();
```

Kurallar:

- **Ad:** `<servis>_<ne>_<birim>` biÃ§iminde olmalÄ±. SayaÃ§lar `_total` ile biter.
- **SÄ±nÄ±rlÄ± label:** label deÄŸerleri sÄ±nÄ±rlÄ± bir kÃ¼me olmalÄ±dÄ±r (event, banka, neden). `transaction_id`, IBAN veya tutar **label yapÄ±lmaz**; her biri yeni bir seri Ã¼retir ve Prometheus'u ÅŸiÅŸirir. TanÄ±msÄ±z deÄŸerler `other` gibi tek bir deÄŸerde toplanmalÄ±dÄ±r (bkz. `Banks.labels`). Transfer bazÄ±nda ayrÄ±ntÄ± gerekiyorsa Ã§Ä±ktÄ± topic'ine yazÄ±lÄ±r, metriÄŸe deÄŸil.
- **GÃ¶rÃ¼nÃ¼rlÃ¼k:** metrik `/metrics` endpoint'inde otomatik gÃ¶rÃ¼nÃ¼r. Kontrol: `curl localhost:2114/metrics | grep consumer_fast_refunds`.

### b) Prometheus'un toplamasÄ±

- Mevcut servislere eklenen metrikler iÃ§in bir ÅŸey yapmak gerekmez; replica'lar zaten scrape ediliyor.
- **Yeni bir servis** iÃ§in [prometheus.yml](./observability/prometheus.yml)'e bir `job` ekleyin. Replica'lÄ± servislerde `dns_sd_configs` kullanÄ±n ve `impl` label'Ä±nÄ± ekleyin. ArdÄ±ndan `docker compose kill -s HUP prometheus` Ã§alÄ±ÅŸtÄ±rÄ±n.
- Kontrol: http://localhost:9090/targets sayfasÄ±nda hedef `UP` olmalÄ±.

### c) (Ä°steÄŸe baÄŸlÄ±) Recording rule

Alarm ve dashboard aynÄ± hesaplamayÄ± kullanacaksa veya sorgu aÄŸÄ±rsa [rules/fast.yml](./observability/prometheus/rules/fast.yml)'e bir kural ekleyin. Ad `seviye:metrik:iÅŸlem` biÃ§iminde olmalÄ± (Ã¶r. `fast:refunds:increase5m`). ArdÄ±ndan:

```bash
docker compose kill -s HUP prometheus
```

### d) Panel eklemek

1. Paneli Grafana'da tasarlayÄ±n.
2. Dashboard'u **Export â†’ Export as JSON** ile dÄ±ÅŸa alÄ±n.
3. JSON'u `observability/grafana/dashboards/<KlasÃ¶r>/` altÄ±ndaki dosyaya yazÄ±n. KlasÃ¶r adÄ± Grafana klasÃ¶rÃ¼ olur.
4. Sorgularda veri kaynaÄŸÄ± olarak `${datasource}` (Prometheus) veya `${loki}` deÄŸiÅŸkenini kullanÄ±n. Uygulama metriklerine `impl="dotnet"` filtresini ekleyin.
5. `docker compose restart grafana` Ã§alÄ±ÅŸtÄ±rÄ±n. Windows bind mount'larÄ±nda dosya deÄŸiÅŸikliÄŸi her zaman algÄ±lanmadÄ±ÄŸÄ± iÃ§in restart en gÃ¼venli yoldur.

Provisioned dashboard'lar Grafana UI'dan doÄŸrudan kaydedilemez. Kaynak her zaman repo'daki JSON dosyasÄ±dÄ±r; deÄŸiÅŸiklik kalÄ±cÄ± olsun diye dosyaya yazÄ±lÄ±r.

### e) Alarm eklemek

1. [fast-alerts.yml](./observability/grafana/provisioning/alerting/fast-alerts.yml)'e mevcut bir kuralÄ± kopyalayarak yeni bir kural ekleyin:
   - **`uid`:** benzersiz olmalÄ±.
   - **Sorgu (`refId: A`):** `instant: true` olmalÄ±.
   - **`datasourceUid`:** `PBFA97CFB590B2093` (Prometheus).
   - **EÅŸik (`refId: C`):** `type: threshold`, `gt` veya `lt`.
2. `for` ve `noDataState` alanlarÄ±nÄ± belirleyin, `severity` label'Ä±nÄ± ekleyin.
3. Dosya baÅŸÄ±ndaki tabloya ve bu dokÃ¼mana satÄ±r ekleyin.
4. `docker compose restart grafana` Ã§alÄ±ÅŸtÄ±rÄ±n. Kontrol: Grafana â†’ Alerting â†’ Alert rules altÄ±nda kuralÄ±n `health` deÄŸeri `ok` olmalÄ±.

### f) Log gÃ¶stermek

UygulamanÄ±n stdout'a yazmasÄ± yeterlidir. Alloy tÃ¼m container'larÄ± otomatik keÅŸfeder ve `container` label'Ä±yla Loki'ye gÃ¶nderir. Sorgu Ã¶rneÄŸi:

```logql
{container=~".*dotnet-consumer.*"} |= "transaction_id=tx-1001"
```

---

## 5. Bilinen kÄ±sÄ±tlar

- **Uygulama sayaÃ§larÄ± restart'ta sÄ±fÄ±rlanÄ±r.** Bu sayaÃ§lar `rate()` / `increase()` ile kullanÄ±ldÄ±ÄŸÄ± iÃ§in grafikler etkilenmez. Restart anÄ±nda stat panelleri kÄ±sa sÃ¼re dÃ¼ÅŸÃ¼k gÃ¶sterebilir. KalÄ±cÄ± toplamlar iÃ§in Kafka offset'leri kullanÄ±lÄ±r.
- **Reconciler sayaÃ§larÄ± "yan etki"dir.** Ã‡Ä±ktÄ± topic'leri exactly-once ile tam doÄŸrudur. Ancak bir transaction yeniden denendiÄŸinde Prometheus sayacÄ± iki kez artabilir; bu nadirdir. Kesin sayÄ± gerektiÄŸinde topic'ler esas alÄ±nÄ±r.
- **State taramasÄ± tÃ¼m state'i gezer.** TakÄ±lÄ±/eÅŸleÅŸmeyen tespiti iÃ§in her 5 saniyede bekleyen kayÄ±tlarÄ±n tamamÄ± taranÄ±r. Bekleyen kayÄ±t sayÄ±sÄ± (normalde yÃ¼zler) Ã§ok bÃ¼yÃ¼rse `Commit / scan latency` panelinden izlenmeli; gerekirse zamana gÃ¶re indekslenmiÅŸ bir store'a geÃ§ilmeli.
- **Reconciler ilk aÃ§Ä±lÄ±ÅŸta geÃ§miÅŸi iÅŸler.** Yeni bir `APPLICATION_ID` ile baÅŸlatÄ±ldÄ±ÄŸÄ±nda topic'leri baÅŸtan okur. EÅŸleÅŸmesi Ã§ok Ã¶nce bitmiÅŸ eski kayÄ±tlar ilk taramada "unmatched" olarak raporlanabilir.
- **"Last week" Ã§izgileri** en az bir haftalÄ±k veri birikene kadar boÅŸtur. Prometheus verisi varsayÄ±lan olarak 15 gÃ¼n tutulur.
- **Eski Go consumer group'u** (`fast-transfer-consumers`) Kafka'da duruyorsa Kafka dashboard'unda gÃ¶rÃ¼nebilir. Alarmlar ve FAST dashboard'larÄ± onu dikkate almaz. Silmek iÃ§in README'deki komut kullanÄ±labilir.
