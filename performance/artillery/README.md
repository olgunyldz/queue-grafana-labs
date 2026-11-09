# Artillery performans testleri

Artillery senaryoları producer API'sine (`http://127.0.0.1:8080`, `producer-lb` arkasındaki replikalar) FAST, Havale ve EFT event'leri gönderir. Her transfer bir **request** ve aynı `transaction_id` ile bir **sonuç** (success / failed) olarak gönderilir. Test verileri (transaction_id, geçerli check digit'li TR IBAN, TRY tutar, banka kodu, hata nedeni) [processors/fast.cjs](processors/fast.cjs) ile sentetik üretilir.

## Ne ölçülür, ne ölçülmez

Artillery yalnızca **HTTP tarafını** doğrular: 202 yanıtları ve yanıt süreleri (`ensure` eklentisi). Saga tarafı (bloke, kesinleşme, yeniden gönderim, manuel inceleme, aşama süreleri) Artillery'de ölçülmez; her koşumdan sonra Grafana'da kontrol edilir:

| Kontrol | Nerede | Beklenen |
|---|---|---|
| Çakışma ve yeniden gönderim | Geliştirici → Saga | 0'a yakın |
| Manuel inceleme | SRE → Saga, uyarı `Transfer saga manual review` | 0 (eksik sonuç senaryosu hariç) |
| Açık bloke tutar | İş Birimi → Para hareketi | Koşum bitince 0 |
| En eski açık bloke | SRE → Saga | Koşum bitince 0 |
| request → BLOCKED p95 | Geliştirici → Aşama süreleri | ~30 s'in altında (checkpoint 10 s) |
| Reconciler lag | Platform · Kafka | Koşum bitince 0 |

## Senaryolar

| Senaryo | Ne yapar | Ne doğrular |
|---|---|---|
| `smoke` | Her endpoint için birkaç transfer, düşük hız (~2/s, 1 dk) | Endpoint'ler 202 döner; her koşumdan önce çalıştırın |
| `saga-load` | Üç akış, 30 transfer/s, 3 dk. Sonuç isteğin 20 sn sonra gelir | Normal yük altında saga kriterleri (yukarıdaki tablo) |
| `saga-spike` | 5 → 120 transfer/s, 5 dk | Kapasite: request → BLOCKED p95'in ve lag'in bozulmaya başladığı hız |
| `saga-early-outcome` | Sonuç isteğin hemen ardından gelir (sonuç bloke onayından önce) | Sonuç tamponlanır, çakışma ve manuel inceleme olmaz |
| `saga-high-failure` | FAST transferlerinin yarısı başarısız, 5 dk | Serbest bırakma yolu: RELEASED → FAILED, bloke tutar 0'a iner |
| `saga-missing-outcomes` | Transferlerin dörtte biri için sonuç gelmez, 12 dk | Açık bloke büyür, 10 dk sonra manuel inceleme; uyarı çalar |
| `request-backlog` | Yalnızca request, 10 → 100/s, 2 dk. Flink durdurulmuş olmalı | Lag birikir; Flink başlatılınca boşalır |
| `low-volume` | ~12 transfer/dk, 7 dk. Yalnızca 09:00–18:00 İstanbul | Düşük hacim uyarıları (mesai saati) |
| `demo-steady` | ~3 transfer/s, varsayılan 1 saat; yaklaşık %1 sonuçsuz. Ctrl+C ile durdurulur | Demo için paneller sürekli dolu kalır; gerçekçi sıra; manuel inceleme birkaç kez görünür |

`saga-load` ve `saga-spike` sonuçların bloke onayından **sonra** geldiği gerçekçi sırayı kullanır. `saga-early-outcome` bilerek sırayı bozar. Diğer senaryolar sonucu 20 sn bekletir.

## Çalıştırma

Önce stack ayakta olmalı (`docker compose up -d`). İlk kez:

```powershell
cd performance/artillery
npm install
```

Tek senaryo:

```powershell
npm run test:smoke
npm run test:saga-load
```

Sırayla birkaç senaryo: `node scripts/run-case.mjs smoke saga-load`. Tüm temel koşum (smoke + saga-load, ~4 dk): `npm run test:all`.

Demo için sürekli veri: `npm run test:demo-steady` (varsayılan 1 saat, Ctrl+C ile durdurulur).

Her koşumun JSON raporu `reports/<senaryo>-<zaman>.json` altına yazılır.

### Kurallar

- Senaryoları **aynı anda** çalıştırmayın; trafikleri birbirine karışır ve Grafana'daki sayılar okunamaz hâle gelir.
- Her koşumdan sonra saga kontrollerini bitirmeden bir sonrakini başlatmayın; önce açık bloke tutarın 0'a indiğini görün.
- `request-backlog` Flink TaskManager'ı durdurur; bu üç akışın da durması demektir. Adımlar senaryo dosyasında yazılı.
- `saga-missing-outcomes` 12 dakika sürer; manuel inceleme ancak 10 dakika sonra görünür.
