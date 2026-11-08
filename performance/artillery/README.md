# Artillery performans ve alarm testleri

Bu proje producer API'sine (`http://127.0.0.1:8080`, `producer-lb` arkasındaki replikalar) FAST, Havale ve EFT event'leri gönderir:

| Akış | Endpoint'ler |
|---|---|
| FAST | `/events/fast/{incoming,outgoing}/{request,success,failed}` |
| Havale | `/events/havale/{request,success,failed}` |
| EFT | `/events/eft/{incoming,outgoing}/{request,success,failed}` |

Test verileri (transaction_id, geçerli check digit'li TR IBAN, TRY tutar, banka kodu, hata nedeni) [processors/fast.cjs](processors/fast.cjs) ile sentetik olarak üretilir. Artillery `2.0.34` sürümüne sabitlenmiştir.

Demo için panoları doldurmak amacıyla basit bir trafik gerekiyorsa Artillery yerine [../demo-traffic/traffic.py](../demo-traffic/traffic.py) kullanılabilir (kurulum gerektirmez).

## Kurulum

```powershell
cd performance/artillery
npm install
```

Önce ana stack çalışıyor olmalı (`docker compose up -d`). Runner Docker komutu çalıştırmaz, servisleri durdurup başlatmaz; gereken durumu testten önce elle hazırlayın.

## Test case'leri

| Case | Akış | Gönderilen event'ler | Amaç / beklenen sonuç |
|---|---|---|---|
| `both-open` | Tümü | Request + sonuç, 5 dk boyunca saniyede 100 sanal kullanıcı | Tüm akışlarda yüksek yük |
| `mixed-high-load` | Tümü | Request + sonuç, 5 dk boyunca saniyede 5 sanal kullanıcı | Tüm akışlarda orta yük, uçtan uca akış |
| `directional-endpoints` | Tümü | Her success / failed endpoint'ine birer istek | Endpoint sözleşmesi smoke testi |
| `request-backlog` | FAST | Yalnızca `fast/incoming/request` (10 → 100 istek/s, 1 dk) | Reconciler durdurulursa `fast_incoming_request` topic'inde lag birikir, 30 s sonra takılan transferler artar |
| `no-request` | FAST | Yalnızca `fast/incoming/success` / `failed`, hiç request yok | Request topic'i sabit kalır; reconciler orphan sonuçları tutar, 10 dk sonra `outcome_without_send` olarak düşer |
| `missing-outcomes` | FAST | Transferlerin ~%25'i için sonuç gönderilmez (8 dk) | `Transfer request/outcome mismatch` alarmı |
| `high-failure` | FAST | Transferlerin %50'si başarısız (7 dk) | `Transfer failed count high` + `Transfer failed/success ratio high` |
| `request-spike` | FAST | 30 transfer/s (7 dk) | `Transfer request volume high` + `Transfer outcome volume high` |
| `low-volume` | FAST | 0,2 transfer/s (7 dk) | `Transfer request / outcome volume low`; **yalnızca 09:00–18:00 İstanbul saatinde** tetiklenir |
| `consumer-only` | — | Consumer `/metrics` | Consumer metrik endpoint'inin erişilebilirliği |

Alarmlar akış başına ayrı instance üretir; bu case'lerde `flow="fast"` instance'ı tetiklenir.

Çalıştırma:

```powershell
npm run test:both-open
npm run test:mixed-high-load
npm run test:directional-endpoints
npm run test:request-backlog
npm run test:no-request
npm run test:missing-outcomes
npm run test:high-failure
npm run test:request-spike
npm run test:low-volume
npm run test:consumer-only
```

Runner her case için `reports/` altında bir JSON raporu üretir. Tüm case'leri sırayla çalıştırmak için `npm run test:all` (alarm case'leri dahil, toplam ~35 dakika).

`consumer-only` case'inin hedefi `http://127.0.0.1:2114`'tür; compose'da consumer portu host'a açık değildir, bu case için portu açmanız gerekir.

### Notlar

- **`request-backlog`:** backlog'un birikmesi için FAST reconciler'ı testten önce durdurun ve sonra başlatın:

  ```powershell
  docker compose stop flink-taskmanager
  npm run test:request-backlog
  docker compose start flink-taskmanager
  ```

  Reconciler çalışıyorsa senaryo yalnızca normal tüketim altında trafik üretir. Diğer akışlar etkilenmez.
- **Süreler:** `missing-outcomes` 8 dakika sürer; fark oranı ~%25 olur, alarm ~1 dakika sonra `pending`, 5 dakika sonra `firing` olur. `high-failure`, `request-spike` ve `low-volume` 7 dakika sürer; alarmlar ~5–6 dakika sonra `firing` olur.
- **Alarm case'leri aynı anda çalıştırılmamalıdır**; trafikleri birbirine karışır.

## Grafana ile izleme

Test sırasında http://localhost:3000 adresinde:

- `FAST / FAST · İş Birimi`: transfer sayıları, başarı oranı, tamamlanma süresi, bankalar (Havale ve EFT için kendi klasörleri).
- `FAST / FAST · Geliştirici`: endpoint başına istek, gecikme ve hatalar; topic akışı ve reconciler lag'i; bekleyen / takılan / orphan kayıtlar.
- `FAST / FAST · SRE` ve `Platform / Platform · Flink`: backpressure, lag, checkpoint ve kaynaklar.
- `Platform / Platform · Servisler` ve `Platform / Platform · Kafka`: replika dağılımı, broker ve partition'lar.

Artillery'nin `http.codes.202`, `http.response_time`, `http.request_rate` ve hata metrikleri JSON raporunda bulunur.
