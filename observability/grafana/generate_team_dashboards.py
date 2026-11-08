"""Generates the team and platform Grafana dashboards.

    python observability/grafana/generate_team_dashboards.py

Writes 11 dashboards:
  {FAST, Havale, EFT} x {Business, Developer, SRE} -> dashboards/<Flow>/<flow>-<team>.json
  Platform: platform-services.json, flink-platform.json -> dashboards/Platform/
(dashboards/Platform/kafka-consumer-groups.json is maintained by hand.)
Edit this script, not the generated JSON, and run it again; Grafana reloads the files within 10 s.

Metric contract (see observability/prometheus.yml):
  reconciler_*{impl="flink", flow="<flow>"}       business metrics of the Flink reconciler
  flink_*{flow="<flow>"}                           Flink job / TaskManager / JobManager metrics
  producer_* / consumer_*{endpoint="/events/<flow>/..."}   mock API and consumer
  kafka_consumergroup_* / kafka_topic_*            kafka-exporter (topics named <flow>_...)
"""

import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "PBFA97CFB590B2093"}
LOKI = {"type": "loki", "uid": "P8E80F9AEF21F6940"}
OUT = Path(__file__).resolve().parent / "dashboards"

FLOWS = {
    "fast": {"name": "FAST", "folder": "FAST", "directions": ["incoming", "outgoing"],
             "about": "FAST: 7/24 anlık transfer sistemi. Gelen (incoming) ve giden (outgoing) yönleri ayrı izlenir."},
    "havale": {"name": "Havale", "folder": "Havale", "directions": [],
               "about": "Havale: aynı banka içi transfer. Tek yönlü akış: request → success / failed."},
    "eft": {"name": "EFT", "folder": "EFT", "directions": ["incoming", "outgoing"],
            "about": "EFT: bankalar arası transfer. Gelen (incoming) ve giden (outgoing) yönleri ayrı izlenir."},
}
DIRECTION_TR = {"incoming": "Gelen", "outgoing": "Giden"}
TEAMS = {
    "business": "İş Birimi",
    "developer": "Geliştirici",
    "sre": "SRE",
}


# --------------------------------------------------------------------------------------------
# Small panel builders
# --------------------------------------------------------------------------------------------
class Board:
    def __init__(self, uid, title, tags, description):
        self.uid, self.title, self.tags, self.description = uid, title, tags, description
        self.panels, self.variables = [], []
        self._id, self._x, self._y, self._row_h = 0, 0, 0, 0

    def _place(self, panel, w, h):
        if self._x + w > 24:
            self._x, self._y = 0, self._y + self._row_h
            self._row_h = 0
        panel["gridPos"] = {"x": self._x, "y": self._y, "w": w, "h": h}
        self._x += w
        self._row_h = max(self._row_h, h)
        self._id += 1
        panel["id"] = self._id
        self.panels.append(panel)
        return panel

    def row(self, title):
        if self._x:
            self._x, self._y, self._row_h = 0, self._y + self._row_h, 0
        self._id += 1
        self.panels.append({"type": "row", "title": title, "id": self._id, "collapsed": False,
                            "gridPos": {"x": 0, "y": self._y, "w": 24, "h": 1}, "panels": []})
        self._y += 1

    def add(self, panel, w, h):
        return self._place(panel, w, h)

    def to_json(self, links):
        return {
            "uid": self.uid,
            "title": self.title,
            "description": self.description,
            "tags": self.tags,
            "editable": True,
            "graphTooltip": 1,
            "schemaVersion": 39,
            "version": 1,
            "refresh": "30s",
            "time": {"from": "now-1h", "to": "now"},
            "timezone": "browser",
            "links": links,
            "templating": {"list": self.variables},
            "annotations": {"list": [{
                "builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                "enable": True, "hide": True, "iconColor": "rgba(0, 211, 255, 1)",
                "name": "Annotations & Alerts", "type": "dashboard"}]},
            "panels": self.panels,
        }


def target(expr, legend="", ref="A", instant=False, fmt=None, ds=PROM):
    t = {"datasource": ds, "refId": ref, "expr": expr, "legendFormat": legend or "__auto",
         "range": not instant, "instant": instant}
    if fmt:
        t["format"] = fmt
    return t


def targets(*items, **kw):
    refs = "ABCDEFGHIJ"
    return [target(e, l, refs[i], **kw) for i, (e, l) in enumerate(items)]


def thresholds(*steps):
    """steps: (color, value) with the first value None."""
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in steps]}


GREEN = thresholds(("green", None))


def stat(title, exprs, unit="short", desc="", th=GREEN, decimals=None, color_mode="value", graph=True,
         text_mode="auto"):
    """text_mode "value_and_name" shows one value per series with its legend (e.g. one per flow)."""
    defaults = {"unit": unit, "thresholds": th, "color": {"mode": "thresholds"}}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "type": "stat", "title": title, "description": desc, "datasource": PROM,
        "targets": targets(*exprs, instant=not graph) if graph is False else targets(*exprs),
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": color_mode, "graphMode": "area" if graph else "none",
                    "textMode": text_mode, "justifyMode": "auto", "orientation": "auto"},
    }


def timeseries(title, exprs, unit="short", desc="", stack=False, th=None, fill=10, overrides=None,
               min_zero=True, legend_calcs=("mean", "max", "lastNotNull")):
    custom = {"drawStyle": "line", "lineWidth": 2, "fillOpacity": fill, "showPoints": "never",
              "spanNulls": True, "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    if th:
        custom["thresholdsStyle"] = {"mode": "dashed"}
    defaults = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"},
                "thresholds": th or GREEN}
    if min_zero:
        defaults["min"] = 0
    return {
        "type": "timeseries", "title": title, "description": desc, "datasource": PROM,
        "targets": targets(*exprs),
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": list(legend_calcs)},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def piechart(title, exprs, unit="short", desc=""):
    return {
        "type": "piechart", "title": title, "description": desc, "datasource": PROM,
        "targets": targets(*exprs, instant=True),
        "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "palette-classic"}}, "overrides": []},
        "options": {"pieType": "donut", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "legend": {"displayMode": "table", "placement": "right", "values": ["value", "percent"]},
                    "tooltip": {"mode": "single"}},
    }


def bargauge(title, exprs, unit="short", desc="", th=GREEN, max_value=None):
    defaults = {"unit": unit, "thresholds": th, "color": {"mode": "thresholds"}, "min": 0}
    if max_value is not None:
        defaults["max"] = max_value
    return {
        "type": "bargauge", "title": title, "description": desc, "datasource": PROM,
        "targets": targets(*exprs, instant=True),
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "valueMode": "color", "namePlacement": "auto", "sizing": "auto"},
    }


def table(title, exprs, renames, units, sort_by, desc=""):
    """Instant queries merged on their common labels (e.g. bank)."""
    tg = [target(e, "", "ABCDEFGHIJ"[i], instant=True, fmt="table") for i, e in enumerate(exprs)]
    overrides = [{"matcher": {"id": "byName", "options": name},
                  "properties": [{"id": "unit", "value": unit}] +
                                ([{"id": "custom.cellOptions", "value": {"type": "color-background"}},
                                  {"id": "thresholds", "value": thresholds(("green", None), ("yellow", 0.05), ("red", 0.15))}]
                                 if unit == "percentunit" else [])}
                 for name, unit in units.items()]
    return {
        "type": "table", "title": title, "description": desc, "datasource": PROM, "targets": tg,
        "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": overrides},
        "options": {"showHeader": True, "sortBy": [{"displayName": sort_by, "desc": True}],
                    "cellHeight": "sm", "footer": {"show": False}},
        "transformations": [
            {"id": "merge", "options": {}},
            {"id": "organize", "options": {"excludeByName": {"Time": True}, "renameByName": renames}},
        ],
    }


def heatmap(title, expr, desc=""):
    return {
        "type": "heatmap", "title": title, "description": desc, "datasource": PROM,
        "targets": [dict(target(expr, "{{le}}"), format="heatmap")],
        "options": {"calculate": False, "yAxis": {"unit": "s", "axisPlacement": "left"},
                    "color": {"mode": "scheme", "scheme": "Oranges", "steps": 64},
                    "cellGap": 1, "tooltip": {"mode": "single", "yHistogram": True},
                    "legend": {"show": True}, "rowsFrame": {"layout": "auto"}},
        "fieldConfig": {"defaults": {}, "overrides": []},
    }


def logs(title, expr, desc=""):
    return {
        "type": "logs", "title": title, "description": desc, "datasource": LOKI,
        "targets": [{"datasource": LOKI, "refId": "A", "expr": expr, "queryType": "range"}],
        "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending",
                    "enableLogDetails": True, "dedupStrategy": "none", "prettifyLogMessage": False},
    }


def log_volume(title, expr, desc=""):
    p = timeseries(title, [], "short", desc, stack=True, fill=60)
    p["datasource"] = LOKI
    p["targets"] = [{"datasource": LOKI, "refId": "A", "expr": expr, "legendFormat": "{{container}}",
                     "queryType": "range"}]
    p["fieldConfig"]["defaults"]["custom"]["drawStyle"] = "bars"
    return p


def alerts(title, label_filter, desc=""):
    return {
        "type": "alertlist", "title": title, "description": desc,
        "options": {"viewMode": "list", "groupMode": "default", "maxItems": 30, "sortOrder": 3,
                    "dashboardAlerts": False, "alertName": "", "showInstances": True,
                    "alertInstanceLabelFilter": label_filter,
                    "stateFilter": {"firing": True, "pending": True, "noData": False, "normal": False, "error": True}},
    }


def text(content):
    return {"type": "text", "title": "", "options": {"mode": "markdown", "content": content},
            "transparent": True}


def custom_var(name, label, options):
    """options: list of (value, text); first is the default."""
    return {
        "name": name, "label": label, "type": "custom", "hide": 0,
        "query": ",".join(f"{t} : {v}" for v, t in options),
        "current": {"text": options[0][1], "value": options[0][0]},
        "options": [{"text": t, "value": v, "selected": i == 0} for i, (v, t) in enumerate(options)],
        "multi": False, "includeAll": False,
    }


# --------------------------------------------------------------------------------------------
# PromQL helpers
# --------------------------------------------------------------------------------------------
def sel(**labels):
    parts = []
    for k, v in labels.items():
        # Regex when the value uses regex syntax ("5..", "[45]..", ".*", "a|b"); "5.0" (le) stays exact.
        op = "=~" if any(c in v for c in "|*+()[") or ".." in v else "="
        parts.append(f'{k}{op}"{v}"')
    return "{" + ", ".join(parts) + "}"


class Q:
    """Query snippets for one flow."""

    def __init__(self, flow):
        self.f = flow
        self.ok = f"{flow}_(.+_)?success"
        self.fail = f"{flow}_(.+_)?failed"
        self.group = f"flink-{flow}-reconciler"
        self.endpoints = f"/events/{flow}/.*"

    def r(self, metric, **labels):
        return f"reconciler_{metric}" + sel(impl="flink", flow=self.f, **labels)

    def transfers(self, window, outcome=None, role="sender", by=""):
        labels = {"role": role}
        if outcome:
            labels["outcome"] = outcome
        b = f" by ({by})" if by else ""
        return f"round(sum{b} (increase({self.r('transfers_total', **labels)}[{window}])))"

    def transfer_rate(self, outcome=None, role="sender", by=""):
        labels = {"role": role}
        if outcome:
            labels["outcome"] = outcome
        b = f" by ({by})" if by else ""
        return f"sum{b} (rate({self.r('transfers_total', **labels)}[$__rate_interval])) * 60"

    def volume(self, window, outcome=None, role="sender", by=""):
        labels = {"role": role}
        if outcome:
            labels["outcome"] = outcome
        b = f" by ({by})" if by else ""
        return f"sum{b} (increase({self.r('volume_try_total', **labels)}[{window}]))"

    def completion_q(self, q, window, by="le", outcome=None):
        labels = {"outcome": outcome} if outcome else {}
        return (f"histogram_quantile({q}, sum by ({by}) "
                f"(rate({self.r('completion_seconds_bucket', **labels)}[{window}])))")


# --------------------------------------------------------------------------------------------
# Dashboards
# --------------------------------------------------------------------------------------------
def business(flow, meta):
    q = Q(flow)
    name = meta["name"]
    b = Board(f"{flow}-business", f"{name} · İş Birimi", [flow, "transfer-team", "business"],
              f"{name} transferlerinin iş görünümü: adet, başarı, hacim, süre, bankalar ve başarısızlık nedenleri.")
    b.variables.append(custom_var("role", "Banka rolü", [("sender", "Gönderen banka"), ("receiver", "Alıcı banka")]))

    b.add(text(f"### {name} · İş Birimi panosu\n{meta['about']} "
               "Sayılar **seçili zaman aralığı** içindir (sağ üstten değiştirilebilir). "
               "Bir transfer, isteği ve sonucu eşleştiğinde **bir kez** sayılır; tutar ve banka bilgisi istekten alınır."), 24, 3)

    b.row("Özet")
    total = q.transfers("$__range")
    ok = q.transfers("$__range", q.ok)
    rate_th = thresholds(("red", None), ("yellow", 0.95), ("green", 0.99))
    b.add(stat("Tamamlanan transfer", [(total, "Adet")], "short",
               "İsteği ve sonucu eşleşen transfer sayısı.", graph=False), 4, 5)
    b.add(stat("Başarı oranı", [(f"{ok} / clamp_min({total}, 1)", "Başarı")], "percentunit",
               "Başarılı sonuçlanan transferlerin oranı. Hedef: %99 ve üzeri.", rate_th, decimals=2, graph=False), 4, 5)
    b.add(stat("Başarılı hacim", [(q.volume("$__range", q.ok), "TRY")], "currencyTRY",
               "Başarılı transferlerin toplam tutarı.", graph=False), 4, 5)
    b.add(stat("Ortalama tutar",
               [(f"sum(increase({q.r('amount_try_sum')}[$__range])) / clamp_min(sum(increase({q.r('amount_try_count')}[$__range])), 1)", "TRY")],
               "currencyTRY", "Transfer başına ortalama tutar.", graph=False), 4, 5)
    b.add(stat("Tamamlanma süresi (p95)", [(q.completion_q(0.95, "$__range"), "p95")], "s",
               "Transferlerin %95'i bu süreden kısa sürede sonuçlandı (istek → sonuç).",
               thresholds(("green", None), ("yellow", 2), ("red", 5)), decimals=2, graph=False), 4, 5)
    b.add(stat("Sonuçsuz kalan istek",
               [(f"round(sum(increase({q.r('unmatched_total', kind='send_without_outcome')}[$__range]))) or vector(0)", "Adet")],
               "short", "10 dakika içinde sonucu gelmeyen istekler. Müşteri tarafında belirsiz kalan işlemlerdir.",
               thresholds(("green", None), ("red", 1)), graph=False), 4, 5)

    b.row("Zaman içinde")
    b.add(timeseries("Dakikada işlem (geçen haftayla)", [
        (q.transfer_rate(q.ok), "Başarılı"),
        (q.transfer_rate(q.fail), "Başarısız"),
        (q.transfer_rate() .replace("[$__rate_interval]", "[$__rate_interval] offset 1w"), "Toplam (geçen hafta)"),
    ], "short", "Dakikada sonuçlanan transfer. Kesikli gri çizgi: geçen haftanın aynı saati.",
        overrides=[{"matcher": {"id": "byName", "options": "Toplam (geçen hafta)"},
                    "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "#8e8e8e"}},
                                   {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 10]}},
                                   {"id": "custom.fillOpacity", "value": 0}]},
                   {"matcher": {"id": "byName", "options": "Başarısız"},
                    "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]},
                   {"matcher": {"id": "byName", "options": "Başarılı"},
                    "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}}]}]), 12, 8)
    b.add(timeseries("Dakikada TRY hacmi", [
        (f"sum(rate({q.r('volume_try_total', role='sender', outcome=q.ok)}[$__rate_interval])) * 60", "Başarılı"),
        (f"sum(rate({q.r('volume_try_total', role='sender', outcome=q.fail)}[$__rate_interval])) * 60", "Başarısız"),
    ], "currencyTRY", "Dakikada sonuçlanan transfer tutarı.", stack=True,
        overrides=[{"matcher": {"id": "byName", "options": "Başarısız"},
                    "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]},
                   {"matcher": {"id": "byName", "options": "Başarılı"},
                    "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}}]}]), 12, 8)
    b.add(timeseries("Başarı oranı", [
        (f"{q.transfer_rate(q.ok)} / clamp_min({q.transfer_rate()}, 1e-9)", "Başarı oranı"),
    ], "percentunit", "Dakikalık başarı oranı. Kesikli çizgiler: %95 (uyarı) ve %99 (hedef).",
        th=thresholds(("red", None), ("yellow", 0.95), ("green", 0.99)), min_zero=False), 12, 7)
    b.add(timeseries("Tamamlanma süresi", [
        (q.completion_q(0.5, "5m"), "Medyan (p50)"),
        (q.completion_q(0.95, "5m"), "p95"),
    ], "s", "İstekten sonuca geçen süre (son 5 dakikalık pencere).",
        th=thresholds(("green", None), ("red", 5))), 12, 7)

    if meta["directions"]:
        b.row("Gelen / giden")
        for d in meta["directions"]:
            d_out = f"{flow}_{d}_.*"
            d_ok = f"{flow}_{d}_success"
            label = DIRECTION_TR[d]
            b.add(stat(f"{label}: transfer", [(q.transfers("$__range", d_out), "Adet")], "short", graph=False), 4, 4)
            b.add(stat(f"{label}: başarı oranı",
                       [(f"{q.transfers('$__range', d_ok)} / clamp_min({q.transfers('$__range', d_out)}, 1)", "Başarı")],
                       "percentunit", th=rate_th, decimals=2, graph=False), 4, 4)
        b.add(piechart("Hacim dağılımı (TRY)", [
            (q.volume("$__range", f"{flow}_{d}_.*"), DIRECTION_TR[d]) for d in meta["directions"]
        ], "currencyTRY", "Seçili aralıktaki toplam tutarın yönlere dağılımı."), 8, 8)
        trend = timeseries("Yöne göre dakikada işlem", [
            (q.transfer_rate(f"{flow}_{d}_.*"), DIRECTION_TR[d]) for d in meta["directions"]
        ], "short", stack=True)
        trend["options"]["legend"] = {"displayMode": "list", "placement": "right", "calcs": []}
        b.add(trend, 16, 4)

    b.row("Bankalar")
    tbl = table("Banka özeti", [
        q.transfers("$__range", role="$role", by="bank"),
        q.transfers("$__range", q.ok, role="$role", by="bank"),
        q.transfers("$__range", q.fail, role="$role", by="bank"),
        f"{q.transfers('$__range', q.fail, role='$role', by='bank')} / clamp_min({q.transfers('$__range', role='$role', by='bank')}, 1)",
        q.volume("$__range", q.ok, role="$role", by="bank"),
    ], {"bank": "Banka", "Value #A": "Transfer", "Value #B": "Başarılı", "Value #C": "Başarısız",
        "Value #D": "Hata oranı", "Value #E": "Başarılı hacim (TRY)"},
        {"Hata oranı": "percentunit", "Başarılı hacim (TRY)": "currencyTRY"}, "Transfer",
        "Seçili banka rolüne (gönderen / alıcı) göre banka bazında özet. Hata oranı %5 üzerinde sarı, %15 üzerinde kırmızı.")
    b.add(tbl, 14, 10)
    b.add(bargauge("En yüksek hata oranı (ilk 5 banka)", [
        (f"topk(5, {q.transfers('$__range', q.fail, role='$role', by='bank')} / clamp_min({q.transfers('$__range', role='$role', by='bank')}, 1))", "{{bank}}"),
    ], "percentunit", "Seçili aralıkta başarısız transfer oranı en yüksek bankalar.",
        thresholds(("green", None), ("yellow", 0.05), ("red", 0.15)), max_value=1), 10, 10)

    b.row("Başarısızlık nedenleri")
    b.add(piechart("Nedenlere göre başarısız transfer", [
        (f"round(sum by (reason) (increase({q.r('failures_total')}[$__range])))", "{{reason}}"),
    ], "short", "Başarısız transferlerin nedenlere dağılımı."), 8, 8)
    b.add(timeseries("Nedenlere göre dakikada başarısız işlem", [
        (f"sum by (reason) (rate({q.r('failures_total')}[$__rate_interval])) * 60", "{{reason}}"),
    ], "short", stack=True), 16, 8)

    b.row("Uyarılar")
    b.add(alerts(f"{name} iş uyarıları", f'{{flow="{flow}", domain="transfer"}}',
                 "Bu akış için tetiklenen veya bekleyen iş uyarıları (hacim, başarı oranı, takılan transferler)."), 24, 6)
    return b


def developer(flow, meta):
    q = Q(flow)
    name = meta["name"]
    b = Board(f"{flow}-developer", f"{name} · Geliştirici", [flow, "transfer-team", "developer"],
              f"{name} event akışı: API, Kafka topic'leri, reconciler iç durumu, Flink throughput ve hata logları.")
    b.add(text(f"### {name} · Geliştirici panosu\n"
               f"Akış: **API** (`/events/{flow}/...`) → **Kafka** (`{flow}_*` topic'leri) → **Flink reconciler** "
               f"(`{q.group}`, `transaction_id` ile eşleştirme) → `{flow}_transfer_matched` / `{flow}_transfer_unmatched`."), 24, 3)

    b.row("API (mock-producer)")
    ep = sel(job="mock-producer", endpoint=q.endpoints)
    b.add(timeseries("Endpoint başına istek / s", [
        (f"sum by (endpoint) (rate(producer_http_requests_total{ep}[$__rate_interval]))", "{{endpoint}}"),
    ], "reqps"), 8, 8)
    b.add(timeseries("Gecikme p95 / p99", [
        (f"histogram_quantile(0.95, sum by (le, endpoint) (rate(producer_http_request_duration_seconds_bucket{ep}[$__rate_interval])))", "p95 {{endpoint}}"),
        (f"histogram_quantile(0.99, sum by (le) (rate(producer_http_request_duration_seconds_bucket{ep}[$__rate_interval])))", "p99 (tümü)"),
    ], "s", "HTTP isteğinin süresi (Kafka istemci kuyruğuna yazma dahil)."), 8, 8)
    b.add(timeseries("HTTP hata kodları ve producer hataları / s", [
        (f"sum by (code) (rate(producer_http_requests_total{sel(job='mock-producer', endpoint=q.endpoints, code='[45]..')}[$__rate_interval]))", "HTTP {{code}}"),
        (f"sum by (reason) (rate(producer_errors_total{ep}[$__rate_interval]))", "producer: {{reason}}"),
    ], "reqps", "4xx: geçersiz istek (validation / invalid_json). 5xx ve producer `kafka*` hataları: Kafka tarafı."), 8, 8)

    b.row("Kafka")
    topics = sel(topic=f"{flow}_.*")
    b.add(timeseries("Topic'e yazılan mesaj / s", [
        (f"sum by (topic) (rate(kafka_topic_partition_current_offset{topics}[$__rate_interval]))", "{{topic}}"),
    ], "short", "Giriş (request/success/failed) ve çıkış (matched/unmatched) topic'leri."), 12, 8)
    b.add(timeseries("Reconciler lag (mesaj)", [
        (f"sum by (topic) (kafka_consumergroup_lag{sel(consumergroup=q.group)})", "{{topic}}"),
    ], "short", f"`{q.group}` consumer group'unun okunmamış mesajları. Flink offset'leri checkpoint'te (10 s) commit eder."), 12, 8)
    b.add(timeseries("Mock consumer: endpoint başına event / s", [
        (f"sum by (endpoint) (rate(consumer_events_total{sel(endpoint=q.endpoints)}[$__rate_interval]))", "{{endpoint}}"),
    ], "short"), 12, 7)
    b.add(timeseries("Mock consumer: Kafka yazma → tüketme gecikmesi p95", [
        (f"histogram_quantile(0.95, sum by (le, endpoint) (rate(consumer_event_latency_seconds_bucket{sel(endpoint=q.endpoints)}[$__rate_interval])))", "{{endpoint}}"),
    ], "s"), 12, 7)

    b.row("Reconciler iç durumu")
    b.add(timeseries("Eşleşen ve süresi dolan / dk", [
        (q.transfer_rate(), "eşleşen"),
        (f"sum by (kind) (rate({q.r('unmatched_total')}[$__rate_interval])) * 60", "unmatched: {{kind}}"),
    ], "short", "unmatched: TTL (10 dk) içinde karşı tarafı gelmeyen kayıtlar."), 8, 8)
    b.add(timeseries("State'teki kayıtlar", [
        (f"sum({q.r('pending')})", "pending (sonuç bekleyen istek)"),
        (f"sum({q.r('stuck')})", "stuck (> 30 s)"),
        (f"sum({q.r('orphan_outcomes')})", "orphan (isteği gelmemiş sonuç)"),
    ], "short", "Checkpoint'ten geri yüklenir; restart sonrası sıfırlanmaz."), 8, 8)
    b.add(timeseries("Duplicate ve geçersiz event / dk", [
        (f"sum by (event) (rate({q.r('duplicates_total')}[$__rate_interval])) * 60", "duplicate: {{event}}"),
        (f"sum(rate({q.r('invalid_events_total')}[$__rate_interval])) * 60", "geçersiz JSON / alan eksik"),
    ], "short", "duplicate: aynı transaction_id ile tekrar gelen event (eşleşmeden önce veya sonraki 10 dk içinde)."), 8, 8)
    b.add(heatmap("Tamamlanma süresi dağılımı",
                  f"sum by (le) (increase({q.r('completion_seconds_bucket')}[$__rate_interval]))",
                  "İstek → sonuç süresinin histogramı (Kafka kayıt zaman damgalarından)."), 12, 8)
    b.add(timeseries("Sonuç türüne göre tamamlanma süresi p95", [
        (q.completion_q(0.95, "5m", by="le, outcome"), "{{outcome}}"),
    ], "s"), 12, 8)

    b.row("Flink job")
    tm = sel(flow=flow)
    b.add(timeseries("Task başına kayıt / s (giriş)", [
        (f"sum by (task_name) (flink_taskmanager_job_task_numRecordsInPerSecond{tm})", "{{task_name}}"),
    ], "short"), 12, 7)
    b.add(timeseries("Kafka sink: gönderilen ve hatalı kayıt / s", [
        (f"sum(rate(flink_taskmanager_job_task_operator_numRecordsSend{tm}[$__rate_interval]))", "gönderilen"),
        (f"sum(rate(flink_taskmanager_job_task_operator_numRecordsSendErrors{tm}[$__rate_interval]))", "hata"),
    ], "short"), 12, 7)

    b.row("Loglar")
    # "Name collision" WARNs (KafkaSink registers pendingCommittables twice) are harmless: filtered out.
    flink_logs = f'{{container=~".*flink-{flow}-.*"}} |~ "(?i)(error|exception|warn)" !~ "Name collision"'
    app_logs = f'{{container=~".*mock-(producer|consumer).*"}} |~ "(?i){flow}" |~ "(?i)(error|warn|fail)"'
    b.add(log_volume("Hata / uyarı log sayısı",
                     f'sum by (container) (count_over_time({flink_logs} [1m]))'), 24, 6)
    b.add(logs(f"Flink ({flow}) hata ve uyarıları", flink_logs), 12, 10)
    b.add(logs(f"API / consumer logları ({flow})", app_logs), 12, 10)
    return b


def sre(flow, meta):
    q = Q(flow)
    name = meta["name"]
    b = Board(f"{flow}-sre", f"{name} · SRE", [flow, "transfer-team", "sre"],
              f"{name} servis sağlığı: erişilebilirlik, SLO'lar, checkpoint, backpressure, lag, kaynaklar ve uyarılar.")
    b.add(text(f"### {name} · SRE panosu\n"
               f"{name} kendi Flink cluster'ında çalışır: `flink-{flow}-jobmanager` (UI) ve `flink-{flow}-taskmanager`. "
               "**SLO'lar:** isteklerin %99,9'u bir sonuca ulaşır, transferlerin %99'u 5 s içinde tamamlanır, API 5xx oranı < %0,1. "
               "İş kaynaklı başarısızlıklar (ör. yetersiz bakiye) SLO ihlali değildir; bilgi amaçlı ayrıca gösterilir."), 24, 3)

    f = sel(flow=flow)
    jm = sel(job="flink-jobmanager", flow=flow)
    tmj = sel(job="flink-reconcilers", flow=flow)
    up_th = thresholds(("red", None), ("green", 1))
    b.row("Durum")
    b.add(stat("JobManager", [(f"min(up{jm})", "")], "short", "Prometheus'un JobManager'a ulaşabilmesi.",
               up_th, graph=False), 3, 4)
    b.add(stat("TaskManager", [(f"min(up{tmj})", "")], "short", th=up_th, graph=False), 3, 4)
    b.add(stat("Çalışan job", [(f"max(flink_jobmanager_numRunningJobs{f})", "")], "short", th=up_th, graph=False), 3, 4)
    b.add(stat("Job uptime", [(f"max(flink_jobmanager_job_uptime{f}) / 1000", "")], "s", graph=False), 3, 4)
    b.add(stat("Restart", [(f"max(increase(flink_jobmanager_job_numRestarts{f}[$__range]))", "")], "short",
               th=thresholds(("green", None), ("yellow", 1), ("red", 3)), decimals=0, graph=False), 3, 4)
    b.add(stat("Boş slot", [(f"max(flink_jobmanager_taskSlotsAvailable{f})", "")], "short",
               "Boş slot kalmazsa bir TaskManager kaybında job yeniden ayağa kalkamaz.",
               thresholds(("red", None), ("green", 1)), graph=False), 3, 4)
    b.add(stat("Toplam lag", [(f"sum(kafka_consumergroup_lag{sel(consumergroup=q.group)})", "")], "short",
               th=thresholds(("green", None), ("yellow", 1000), ("red", 10000))), 3, 4)
    b.add(stat("Checkpoint yaşı", [(f"time() - max(flink_jobmanager_job_lastCheckpointCompletedTimestamp{f}) / 1000", "")],
               "s", "Checkpoint aralığı 10 s; 60 s üzeri sorunlu.",
               thresholds(("green", None), ("yellow", 30), ("red", 60)), graph=False), 3, 4)

    b.row("SLO (seçili aralık)")
    total = q.transfers("$__range")
    slo_th = thresholds(("red", None), ("yellow", 0.99), ("green", 0.999))
    unmatched_send = f"(sum(increase({q.r('unmatched_total', kind='send_without_outcome')}[$__range])) or vector(0))"
    b.add(stat("Sonuca ulaşan istek (SLO ≥ %99,9)",
               [(f"{total} / clamp_min({total} + {unmatched_send}, 1)", "")],
               "percentunit", "Eşleşen / (eşleşen + 10 dk içinde sonucu gelmeyen). Teknik başarı ölçüsü.",
               slo_th, decimals=3, graph=False), 6, 4)
    b.add(stat("5 s içinde tamamlanan (SLO ≥ %99)",
               [(f"sum(increase({q.r('completion_seconds_bucket', le='5.0')}[$__range])) / clamp_min(sum(increase({q.r('completion_seconds_count')}[$__range])), 1)", "")],
               "percentunit", "Kafka kayıt zamanına göre istek → sonuç süresi.", slo_th, decimals=2, graph=False), 6, 4)
    api_total = f"sum(increase(producer_http_requests_total{sel(job='mock-producer', endpoint=q.endpoints)}[$__range]))"
    api_5xx = f"sum(increase(producer_http_requests_total{sel(job='mock-producer', endpoint=q.endpoints, code='5..')}[$__range]))"
    b.add(stat("API başarı (SLO ≥ %99,9)", [(f"1 - (({api_5xx}) or vector(0)) / clamp_min({api_total}, 1)", "")],
               "percentunit", "5xx dışındaki yanıtların oranı.", slo_th, decimals=3, graph=False), 6, 4)
    b.add(stat("İş kaynaklı başarısızlık (bilgi)",
               [(f"{q.transfers('$__range', q.fail)} / clamp_min({total}, 1)", "")],
               "percentunit", "Başarısız sonuçlanan transferlerin oranı (yetersiz bakiye, limit vb.). SLO'ya dahil değildir.",
               thresholds(("blue", None)), decimals=2, graph=False), 6, 4)

    b.row("İşleme ve gecikme")
    b.add(timeseries("Giriş / çıkış kaydı / s", [
        (f"sum(flink_taskmanager_job_task_numRecordsInPerSecond{sel(flow=flow, task_name='Source.*')})", "Kafka'dan okunan"),
        (f"sum(rate(flink_taskmanager_job_task_operator_numRecordsSend{f}[$__rate_interval]))", "Kafka'ya yazılan"),
    ], "short"), 8, 8)
    b.add(timeseries("Backpressure ve meşguliyet (ms/s)", [
        (f"max by (task_name) (flink_taskmanager_job_task_backPressuredTimeMsPerSecond{f})", "backpressure {{task_name}}"),
        (f"max by (task_name) (flink_taskmanager_job_task_busyTimeMsPerSecond{f})", "busy {{task_name}}"),
    ], "ms", "1000 ms/s = sürekli. Backpressure > 600 ms/s uyarı üretir.",
        th=thresholds(("green", None), ("red", 600))), 8, 8)
    b.add(timeseries("Consumer lag", [
        (f"sum by (topic) (kafka_consumergroup_lag{sel(consumergroup=q.group)})", "{{topic}}"),
    ], "short", th=thresholds(("green", None), ("red", 10000))), 8, 8)
    b.add(timeseries("Tamamlanma süresi p99", [
        (q.completion_q(0.99, "5m"), "p99"),
    ], "s", "SLO: 5 s.", th=thresholds(("green", None), ("red", 5))), 12, 7)
    b.add(timeseries("Takılan transfer (> 30 s)", [
        (f"sum({q.r('stuck')})", "stuck"),
    ], "short", "50 üzerinde 5 dakika kalırsa uyarı.", th=thresholds(("green", None), ("red", 50))), 12, 7)

    b.row("Checkpoint")
    b.add(timeseries("Checkpoint süresi", [
        (f"max(flink_jobmanager_job_lastCheckpointDuration{f})", "son checkpoint"),
    ], "ms", th=thresholds(("green", None), ("red", 30000))), 8, 7)
    b.add(timeseries("Checkpoint boyutu", [
        (f"max(flink_jobmanager_job_lastCheckpointFullSize{f})", "tam boyut"),
        (f"max(flink_jobmanager_job_lastCheckpointSize{f})", "son checkpoint"),
    ], "bytes", "State büyüklüğünün göstergesi (bekleyen kayıtlar + 10 dk'lık eşleşmiş ID'ler)."), 8, 7)
    b.add(timeseries("Başarısız checkpoint ve restart", [
        (f"max(increase(flink_jobmanager_job_numberOfFailedCheckpoints{f}[5m]))", "başarısız checkpoint (5 dk)"),
        (f"max(increase(flink_jobmanager_job_numRestarts{f}[5m]))", "restart (5 dk)"),
    ], "short", "Job başlangıcındaki ilk checkpoint, task'lar hazır olmadığından başarısız olabilir."), 8, 7)

    b.row("Kaynaklar")
    b.add(timeseries("CPU", [
        (f"max by (host) (flink_taskmanager_Status_JVM_CPU_Load{f})", "TaskManager {{host}}"),
        (f"max(flink_jobmanager_Status_JVM_CPU_Load{f})", "JobManager"),
    ], "percentunit", th=thresholds(("green", None), ("red", 0.85))), 8, 7)
    b.add(timeseries("Heap kullanımı", [
        (f"max by (host) (flink_taskmanager_Status_JVM_Memory_Heap_Used{f} / flink_taskmanager_Status_JVM_Memory_Heap_Max{f})", "TaskManager {{host}}"),
        (f"max(flink_jobmanager_Status_JVM_Memory_Heap_Used{f} / flink_jobmanager_Status_JVM_Memory_Heap_Max{f})", "JobManager"),
    ], "percentunit", th=thresholds(("green", None), ("red", 0.85))), 8, 7)
    b.add(timeseries("GC süresi (ms/s)", [
        (f"max by (host) (flink_taskmanager_Status_JVM_GarbageCollector_All_TimeMsPerSecond{f})", "TaskManager {{host}}"),
    ], "ms"), 8, 7)

    b.row("Uyarılar ve loglar")
    b.add(alerts(f"{name} uyarıları (iş + Flink)", f'{{flow="{flow}"}}',
                 "Bu akışa ait tüm firing / pending uyarılar."), 10, 9)
    b.add(log_volume("Hata / uyarı log sayısı",
                     f'sum by (container) (count_over_time({{container=~".*flink-{flow}-.*"}} |~ "(?i)(error|exception)" [1m]))'), 14, 9)
    return b


BUILDERS = {"business": business, "developer": developer, "sre": sre}


# --------------------------------------------------------------------------------------------
# Platform dashboards (shared by all flows)
# --------------------------------------------------------------------------------------------
FLOW_FROM_ENDPOINT = '"flow", "$1", "endpoint", "/events/([^/]+)/.*"'
MOCK_GROUPS = "fast_incoming|fast_outgoing|havale|eft_incoming|eft_outgoing"


def flow_var():
    v = custom_var("flow", "Akış", [("fast", "FAST"), ("havale", "Havale"), ("eft", "EFT")])
    v.update({"multi": True, "includeAll": True, "allValue": "fast|havale|eft",
              "current": {"text": ["All"], "value": ["$__all"]}})
    for o in v["options"]:
        o["selected"] = False
    v["options"].insert(0, {"text": "All", "value": "$__all", "selected": True})
    return v


def platform_services():
    b = Board("platform-services", "Platform · Servisler", ["platform"],
              "Tüm akışların kullandığı mock API (producer) ve consumer replikalarının teknik sağlığı.")
    b.add(text("### Platform · Servisler\n"
               "`mock-producer` (2 replika, `producer-lb` arkasında) tüm akışların HTTP event API'sidir; "
               "`mock-consumer` her akışın topic'lerini kendi consumer group'uyla okur. "
               "Akışa özel API ve consumer detayları ilgili **Geliştirici** panosundadır; bu pano replika ve runtime görünümüdür."), 24, 3)
    prod = sel(job="mock-producer")
    cons = sel(job="mock-consumer")
    up_th = thresholds(("red", None), ("green", 1))

    b.row("Replikalar")
    b.add(stat("Producer replika", [(f"sum(up{prod})", "")], "short", "Prometheus'un ulaşabildiği producer replikaları.",
               up_th, graph=False), 4, 4)
    b.add(stat("Consumer replika", [(f"sum(up{cons})", "")], "short", th=up_th, graph=False), 4, 4)
    b.add(stat("API hata oranı (5xx, 5 dk)",
               [(f"(sum(rate(producer_http_requests_total{sel(job='mock-producer', code='5..')}[5m])) or vector(0)) / clamp_min(sum(rate(producer_http_requests_total{prod}[5m])), 1e-9)", "")],
               "percentunit", th=thresholds(("green", None), ("yellow", 0.001), ("red", 0.01)), decimals=2, graph=False), 4, 4)
    b.add(stat("API p95 (5 dk)",
               [(f"histogram_quantile(0.95, sum by (le) (rate(producer_http_request_duration_seconds_bucket{prod}[5m])))", "")],
               "s", th=thresholds(("green", None), ("yellow", 0.1), ("red", 0.5)), decimals=3, graph=False), 4, 4)
    b.add(stat("Consumer hata (5 dk)", [(f"round(sum(increase(consumer_errors_total{cons}[5m]))) or vector(0)", "")],
               "short", th=thresholds(("green", None), ("red", 1)), graph=False), 4, 4)
    b.add(stat("Consumer group üyeleri", [(f"sum by (consumergroup) (kafka_consumergroup_members{sel(consumergroup=MOCK_GROUPS)})", "{{consumergroup}}")],
               "short", "Mock consumer'ın akış başına consumer group'ları.", up_th, graph=False, text_mode="value_and_name"), 4, 4)

    b.row("Producer (API)")
    b.add(timeseries("Akışa göre istek / s", [
        (f"sum by (flow) (label_replace(rate(producer_http_requests_total{prod}[$__rate_interval]), {FLOW_FROM_ENDPOINT}))", "{{flow}}"),
    ], "reqps", "Tüm endpoint'lerin akışa göre toplamı.", stack=True), 8, 8)
    b.add(timeseries("Replika başına istek / s", [
        (f"sum by (instance) (rate(producer_http_requests_total{prod}[$__rate_interval]))", "{{instance}}"),
    ], "reqps", "Yük dengeleyicinin (nginx) replikalar arasında dağıtımı."), 8, 8)
    b.add(timeseries("Hata oranı: 4xx (validation) / 5xx (Kafka)", [
        (f"(sum(rate(producer_http_requests_total{sel(job='mock-producer', code='4..')}[$__rate_interval])) or vector(0)) / clamp_min(sum(rate(producer_http_requests_total{prod}[$__rate_interval])), 1e-9)", "4xx"),
        (f"(sum(rate(producer_http_requests_total{sel(job='mock-producer', code='5..')}[$__rate_interval])) or vector(0)) / clamp_min(sum(rate(producer_http_requests_total{prod}[$__rate_interval])), 1e-9)", "5xx"),
    ], "percentunit", th=thresholds(("green", None), ("red", 0.01))), 8, 8)

    b.row("Consumer")
    b.add(timeseries("Replika ve group başına event / s", [
        (f"sum by (instance, consumergroup) (rate(consumer_partition_events_total{cons}[$__rate_interval]))", "{{consumergroup}} @ {{instance}}"),
    ], "short"), 8, 8)
    b.add(timeseries("Replika başına aktif partition", [
        (f"count by (instance, consumergroup) (sum by (instance, consumergroup, topic, partition) (rate(consumer_partition_events_total{cons}[5m])) > 0)", "{{consumergroup}} @ {{instance}}"),
    ], "short", "Son 5 dakikada mesaj gelen partition sayısı (rebalance sonrası dağılımı gösterir)."), 8, 8)
    b.add(timeseries("Kafka → consumer gecikmesi p95", [
        (f"histogram_quantile(0.95, sum by (le, instance) (rate(consumer_event_latency_seconds_bucket{cons}[$__rate_interval])))", "{{instance}}"),
    ], "s"), 8, 8)

    b.row("Runtime (.NET)")
    dn = sel(impl="dotnet")
    b.add(timeseries("Managed heap", [(f"dotnet_total_memory_bytes{dn}", "{{job}} {{instance}}")], "bytes"), 6, 7)
    b.add(timeseries("Working set", [(f"process_working_set_bytes{dn}", "{{job}} {{instance}}")], "bytes"), 6, 7)
    b.add(timeseries("CPU", [(f"rate(process_cpu_seconds_total{dn}[$__rate_interval])", "{{job}} {{instance}}")], "percentunit"), 6, 7)
    b.add(timeseries("GC / s", [(f"sum by (job, instance) (rate(dotnet_collection_count_total{dn}[$__rate_interval]))", "{{job}} {{instance}}")], "short"), 6, 7)

    b.row("Loglar")
    app = '{container=~".*mock-(producer|consumer).*"}'
    b.add(log_volume("Hata / uyarı log sayısı",
                     f'sum by (container) (count_over_time({app} |~ "(?i)(fail|error|warn|crit)" [1m]))'), 24, 6)
    b.add(logs("Hata ve uyarılar", f'{app} |~ "(?i)(fail|error|warn|crit)"'), 24, 10)
    return b


def flink_platform():
    b = Board("flink-platform", "Platform · Flink (tüm akışlar)", ["platform"],
              "FAST, Havale ve EFT Flink cluster'larının yan yana sağlık görünümü.")
    b.variables.append(flow_var())
    b.add(text("### Platform · Flink\n"
               "Her akış kendi Flink cluster'ında çalışır (application mode): `flink-<akış>-jobmanager` + `flink-<akış>-taskmanager`. "
               "Bu pano üç cluster'ı yan yana gösterir; bir akışın ayrıntısı için o akışın **SRE** panosuna geçin "
               "(UI: FAST `:18082`, Havale `:18083`, EFT `:18084`)."), 24, 3)
    f = '{flow=~"$flow"}'
    jm = '{job="flink-jobmanager", flow=~"$flow"}'
    tm = '{job="flink-reconcilers", flow=~"$flow"}'
    up_th = thresholds(("red", None), ("green", 1))

    b.row("Cluster durumu (akış başına)")
    b.add(stat("JobManager", [(f"min by (flow) (up{jm})", "{{flow}}")], "short", th=up_th, graph=False, text_mode="value_and_name"), 6, 4)
    b.add(stat("TaskManager", [(f"min by (flow) (up{tm})", "{{flow}}")], "short", th=up_th, graph=False, text_mode="value_and_name"), 6, 4)
    b.add(stat("Çalışan job", [(f"max by (flow) (flink_jobmanager_numRunningJobs{f})", "{{flow}}")], "short", th=up_th, graph=False, text_mode="value_and_name"), 6, 4)
    b.add(stat("Boş slot", [(f"max by (flow) (flink_jobmanager_taskSlotsAvailable{f})", "{{flow}}")], "short",
               "0 ise bir TaskManager kaybında job yeniden ayağa kalkamaz.", thresholds(("red", None), ("green", 1)),
               graph=False, text_mode="value_and_name"), 6, 4)
    b.add(stat("Job uptime", [(f"max by (flow) (flink_jobmanager_job_uptime{f}) / 1000", "{{flow}}")], "s", graph=False, text_mode="value_and_name"), 6, 4)
    b.add(stat("Restart (aralıkta)", [(f"max by (flow) (increase(flink_jobmanager_job_numRestarts{f}[$__range]))", "{{flow}}")], "short",
               th=thresholds(("green", None), ("yellow", 1), ("red", 3)), decimals=0, graph=False, text_mode="value_and_name"), 6, 4)
    b.add(stat("Checkpoint yaşı", [(f"time() - max by (flow) (flink_jobmanager_job_lastCheckpointCompletedTimestamp{f}) / 1000", "{{flow}}")], "s",
               th=thresholds(("green", None), ("yellow", 30), ("red", 60)), graph=False, text_mode="value_and_name"), 6, 4)
    b.add(stat("Reconciler lag", [(f'sum by (flow) (label_replace(kafka_consumergroup_lag{{consumergroup=~"flink-($flow)-reconciler"}}, "flow", "$1", "consumergroup", "flink-(.+)-reconciler"))', "{{flow}}")],
               "short", th=thresholds(("green", None), ("yellow", 1000), ("red", 10000)), graph=False, text_mode="value_and_name"), 6, 4)

    b.row("İşleme")
    b.add(timeseries("Kafka'dan okunan kayıt / s", [
        (f'sum by (flow) (flink_taskmanager_job_task_numRecordsInPerSecond{{flow=~"$flow", task_name=~"Source.*"}})', "{{flow}}"),
    ], "short"), 8, 8)
    b.add(timeseries("Backpressure (en yüksek task, ms/s)", [
        (f"max by (flow) (flink_taskmanager_job_task_backPressuredTimeMsPerSecond{f})", "{{flow}}"),
    ], "ms", "600 ms/s üzeri uyarı.", th=thresholds(("green", None), ("red", 600))), 8, 8)
    b.add(timeseries("Meşguliyet (en yüksek task, ms/s)", [
        (f"max by (flow) (flink_taskmanager_job_task_busyTimeMsPerSecond{f})", "{{flow}}"),
    ], "ms", "1000 ms/s = task sürekli çalışıyor; kapasite sınırı."), 8, 8)

    b.row("Checkpoint")
    b.add(timeseries("Checkpoint süresi", [
        (f"max by (flow) (flink_jobmanager_job_lastCheckpointDuration{f})", "{{flow}}"),
    ], "ms", th=thresholds(("green", None), ("red", 30000))), 8, 7)
    b.add(timeseries("Checkpoint boyutu", [
        (f"max by (flow) (flink_jobmanager_job_lastCheckpointFullSize{f})", "{{flow}}"),
    ], "bytes"), 8, 7)
    b.add(timeseries("Başarısız checkpoint (5 dk)", [
        (f"max by (flow) (increase(flink_jobmanager_job_numberOfFailedCheckpoints{f}[5m]))", "{{flow}}"),
    ], "short", "Job başlangıcındaki ilk checkpoint, task'lar hazır olmadığından başarısız olabilir."), 8, 7)

    b.row("Kaynaklar")
    b.add(timeseries("TaskManager CPU", [
        (f"max by (flow) (flink_taskmanager_Status_JVM_CPU_Load{f})", "{{flow}}"),
    ], "percentunit", th=thresholds(("green", None), ("red", 0.85))), 6, 7)
    b.add(timeseries("TaskManager heap", [
        (f"max by (flow) (flink_taskmanager_Status_JVM_Memory_Heap_Used{f} / flink_taskmanager_Status_JVM_Memory_Heap_Max{f})", "{{flow}}"),
    ], "percentunit", th=thresholds(("green", None), ("red", 0.85))), 6, 7)
    b.add(timeseries("JobManager heap", [
        (f"max by (flow) (flink_jobmanager_Status_JVM_Memory_Heap_Used{f} / flink_jobmanager_Status_JVM_Memory_Heap_Max{f})", "{{flow}}"),
    ], "percentunit", th=thresholds(("green", None), ("red", 0.85))), 6, 7)
    b.add(timeseries("GC süresi (ms/s)", [
        (f"max by (flow) (flink_taskmanager_Status_JVM_GarbageCollector_All_TimeMsPerSecond{f})", "{{flow}}"),
    ], "ms"), 6, 7)

    b.row("Uyarılar")
    b.add(alerts("Flink uyarıları (tüm akışlar)", '{domain="flink"}',
                 "Flink altyapı uyarıları; her uyarı etkilenen akışı flow etiketiyle gösterir."), 24, 7)
    return b


PLATFORM_LINKS = [
    {"title": "Platform panoları", "type": "dashboards", "tags": ["platform"], "asDropdown": True,
     "includeVars": False, "keepTime": True, "icon": "external link"},
]


def links(flow, meta):
    return [
        {"title": f"{meta['name']} panoları", "type": "dashboards", "tags": [flow], "asDropdown": True,
         "includeVars": False, "keepTime": True, "icon": "external link"},
        {"title": "Tüm akış panoları", "type": "dashboards", "tags": ["transfer-team"], "asDropdown": True,
         "includeVars": False, "keepTime": True, "icon": "external link"},
    ] + PLATFORM_LINKS


def write(board, path, board_links):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(board.to_json(board_links), ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8", newline="\n")
    print(f"wrote {path.relative_to(OUT.parent.parent.parent)}  ({len(board.panels)} panels)")


def main():
    for flow, meta in FLOWS.items():
        for team, build in BUILDERS.items():
            write(build(flow, meta), OUT / meta["folder"] / f"{flow}-{team}.json", links(flow, meta))

    sre_links = [{"title": f"{m['name']} · SRE", "type": "link", "url": f"/d/{f}-sre", "icon": "dashboard",
                  "keepTime": True, "targetBlank": False} for f, m in FLOWS.items()]
    write(platform_services(), OUT / "Platform" / "platform-services.json", PLATFORM_LINKS)
    write(flink_platform(), OUT / "Platform" / "flink-platform.json", PLATFORM_LINKS + sre_links)


if __name__ == "__main__":
    main()
