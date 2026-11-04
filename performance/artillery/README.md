# Artillery performans testleri

Bu proje producer API'sine FAST event endpoint'leri (`/events/fast_send`, `/events/fast_send_success`, `/events/fast_send_failed`) üzerinden yük gönderir. Test verileri (transaction_id, geçerli check digit'li TR IBAN, TRY tutar, banka kodu, hata nedeni) [processors/fast.cjs](processors/fast.cjs) ile sentetik olarak üretilir. Artillery güncel `2.0.34` sürümüne sabitlenmiştir.

## Kurulum

```powershell
cd performance/artillery
npm install
```

Önce ana stack çalışıyor olmalı:

```powershell
docker compose up -d
```

## Test case'leri

| Case | Producer | Consumer | Gönderilen event'ler | Amaç |
|---|---|---|---|---|
| `both-open` | Açık | Açık | `fast_send` → `%90 fast_send_success` / `%10 fast_send_failed` | Normal uçtan uca FAST akışı |
| `fast-send-backlog` | Açık | Kapalı | Sadece `fast_send` (10 → 100 istek/s) | `fast_send` topic'inde backlog (lag) biriktirme |
| `no-fast-send` | Açık | Açık | Sadece `fast_send_success` / `fast_send_failed`, **hiç `fast_send` yok** | `fast_send` gitmeden sonuç event'lerinin akışı |
| `missing-outcomes` | Açık | Açık | `fast_send`'lerin ~%25'i için sonuç gönderilmez (8 dk) | `FAST send/outcome mismatch` alert'ini tetikleme |
| `high-failure` | Açık | Açık | Transferlerin %50'si `fast_send_failed` (7 dk) | `FAST failed count high` + `FAST failed/success ratio high` |
| `fast-send-spike` | Açık | Açık | 30 transfer/s (7 dk) | `FAST send volume high` + `FAST outcome volume high` |
| `low-volume` | Açık | Açık | 0.2 transfer/s (7 dk) | `FAST send/outcome volume low`; **sadece 09:00–18:00 İstanbul saatinde** tetiklenir |
| `consumer-only` | Kapalı | Açık | — (consumer `/healthz` ve `/metrics`) | Consumer health/metrics erişilebilirliği |

Çalıştırma:

```powershell
npm run test:both-open
npm run test:fast-send-backlog
npm run test:no-fast-send
npm run test:missing-outcomes
npm run test:high-failure
npm run test:fast-send-spike
npm run test:low-volume
npm run test:consumer-only
```

Her case başlamadan önce runner işleri şu sırayla yapar:

1. `dotnet-producer` ve `dotnet-consumer` servislerini (tüm replica'larıyla) durdurur.
2. Seçilen servisleri tekrar açar (producer için `producer-lb` ile birlikte).
3. `/healthz` hazır olana kadar bekler.
4. Testi çalıştırır ve sonunda `reports/` altında JSON raporu üretir.

Producer hedefi load balancer'dır (http://localhost:18081). `consumer-only` case'inde hedef ilk consumer replica'sıdır (http://localhost:2114).

Tüm case'leri sırayla çalıştırmak için:

```powershell
npm run test:all
```

`fast-send-backlog` testi consumer'ı kapalı tutarak `fast_send` topic'inde backlog biriktirir. Sonrasında `npm run test:both-open` veya `docker compose up -d dotnet-consumer` çalıştırıldığında consumer backlog'u tüketir; Grafana'da `fast_send backlog` ve `Lag per event topic` düşer, gecikme p95 ise geçici olarak yükselir.

`missing-outcomes` testi 8 dakika sürer: fark oranı ~%25 olur, alert yaklaşık 1 dakika sonra `pending`, 5 dakika sonra `firing` olur. `high-failure`, `fast-send-spike` ve `low-volume` 7 dakika sürer; ilgili alert'ler önce `pending`, yaklaşık 5–6 dakika sonra `firing` olur. Alert testleri aynı anda çalıştırılmamalıdır, çünkü trafikleri birbirine karışır. `test:all` bu case'leri de içerdiğinden toplam süre ~35 dakikayı bulur.

`no-fast-send` testinde `fast_send` sayacı ve topic offset'i sabit kalır; yalnızca `fast_send_success` / `fast_send_failed` artar.

## Grafana ile izleme

Test sırasında http://localhost:3000 adresindeki dashboard'larda şunları izle:

- `FAST / FAST Operations`: transfer sayıları, başarı oranı, alert eşikleri, tamamlanma süresi, bankalar
- `FAST / FAST Services`: replica başına istek ve tüketim, API süreleri, hatalar ve loglar
- `Kafka / Kafka Consumer Groups & Broker`: `fast_send` backlog'u (lag), partition'lar ve broker

Artillery'nin `http.codes.202`, `http.response_time`, `http.request_rate` ve hata metrikleri JSON raporunda bulunur.
