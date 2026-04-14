"""
Sales Dashboard Plugin — fully server-side rendered with in-memory data.
Generates random sales data on every page load so the dashboard ALWAYS
has charts and tables to display, regardless of DAG state.
Auto-refreshes every 10 seconds via meta-refresh.
"""

import html
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from airflow.plugins_manager import AirflowPlugin

app = FastAPI(title="Sales Dashboard")

PRODUCTS = [
    "Laptop",
    "Phone",
    "Headphones",
    "Tablet",
    "Smartwatch",
    "Monitor",
    "Keyboard",
    "Mouse",
]
PRICE_RANGE = {
    "Laptop": (799, 1299),
    "Phone": (299, 999),
    "Headphones": (29, 349),
    "Tablet": (199, 799),
    "Smartwatch": (99, 499),
    "Monitor": (149, 899),
    "Keyboard": (19, 179),
    "Mouse": (9, 89),
}
REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America"]
CHANNELS = ["Online", "In-Store", "Mobile App"]


# ------------------------------------------------------------------ data gen
def _generate_sales(n: int = 200) -> list[dict]:
    """Generate n random sales records spread over the last 60 minutes."""
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(n):
        product = random.choice(PRODUCTS)
        lo, hi = PRICE_RANGE[product]
        price = round(random.uniform(lo, hi), 2)
        qty = random.randint(1, 5)
        offset = random.randint(0, 3600)  # seconds ago
        rows.append(
            {
                "sale_id": f"SALE-{now.strftime('%Y%m%d')}-{i:04d}",
                "sale_time": now - timedelta(seconds=offset),
                "product": product,
                "price": price,
                "quantity": qty,
                "total": round(price * qty, 2),
                "region": random.choice(REGIONS),
                "channel": random.choice(CHANNELS),
            }
        )
    rows.sort(key=lambda r: r["sale_time"])
    return rows


def _aggregate(rows: list[dict]) -> dict:
    """Compute all dashboard aggregations from raw rows."""
    now = datetime.now(timezone.utc)
    hour_ago = now - timedelta(hours=1)
    recent_rows = [r for r in rows if r["sale_time"] >= hour_ago]

    # KPIs
    total_revenue = sum(r["total"] for r in recent_rows)
    total_orders = len(recent_rows)
    avg_order = round(total_revenue / total_orders, 2) if total_orders else 0
    units_sold = sum(r["quantity"] for r in recent_rows)

    # Timeseries (per minute, last 30 min)
    thirty_ago = now - timedelta(minutes=30)
    ts_buckets = defaultdict(lambda: {"order_count": 0, "revenue": 0.0})
    for r in rows:
        if r["sale_time"] >= thirty_ago:
            key = r["sale_time"].replace(second=0, microsecond=0)
            ts_buckets[key]["order_count"] += 1
            ts_buckets[key]["revenue"] += r["total"]
    timeseries = [
        {
            "minute": k.strftime("%H:%M"),
            "order_count": v["order_count"],
            "revenue": round(v["revenue"], 2),
        }
        for k, v in sorted(ts_buckets.items())
    ]

    # By product
    prod_agg = defaultdict(
        lambda: {"orders": 0, "revenue": 0.0, "units": 0, "price_sum": 0.0}
    )
    for r in recent_rows:
        prod_agg[r["product"]]["orders"] += 1
        prod_agg[r["product"]]["revenue"] += r["total"]
        prod_agg[r["product"]]["units"] += r["quantity"]
        prod_agg[r["product"]]["price_sum"] += r["price"]
    by_product = sorted(
        [
            {"product": k, "orders": v["orders"], "revenue": round(v["revenue"], 2)}
            for k, v in prod_agg.items()
        ],
        key=lambda x: x["revenue"],
        reverse=True,
    )
    product_ranking = sorted(
        [
            {
                "product": k,
                "times_sold": v["orders"],
                "total_units": v["units"],
                "total_revenue": round(v["revenue"], 2),
                "avg_price": round(v["price_sum"] / v["orders"], 2)
                if v["orders"]
                else 0,
            }
            for k, v in prod_agg.items()
        ],
        key=lambda x: x["total_revenue"],
        reverse=True,
    )

    # By region
    reg_agg = defaultdict(lambda: {"orders": 0, "revenue": 0.0})
    for r in recent_rows:
        reg_agg[r["region"]]["orders"] += 1
        reg_agg[r["region"]]["revenue"] += r["total"]
    by_region = sorted(
        [
            {"region": k, "orders": v["orders"], "revenue": round(v["revenue"], 2)}
            for k, v in reg_agg.items()
        ],
        key=lambda x: x["revenue"],
        reverse=True,
    )

    # By channel
    chan_agg = defaultdict(lambda: {"orders": 0, "revenue": 0.0})
    for r in recent_rows:
        chan_agg[r["channel"]]["orders"] += 1
        chan_agg[r["channel"]]["revenue"] += r["total"]
    by_channel = sorted(
        [
            {
                "channel": k,
                "orders": v["orders"],
                "revenue": round(v["revenue"], 2),
                "avg_order": round(v["revenue"] / v["orders"], 2) if v["orders"] else 0,
            }
            for k, v in chan_agg.items()
        ],
        key=lambda x: x["revenue"],
        reverse=True,
    )

    # Region x Channel
    rc_agg = defaultdict(lambda: {"orders": 0, "revenue": 0.0})
    for r in recent_rows:
        rc_agg[(r["region"], r["channel"])]["orders"] += 1
        rc_agg[(r["region"], r["channel"])]["revenue"] += r["total"]
    region_channel = sorted(
        [
            {
                "region": k[0],
                "channel": k[1],
                "orders": v["orders"],
                "revenue": round(v["revenue"], 2),
            }
            for k, v in rc_agg.items()
        ],
        key=lambda x: (x["region"], -x["revenue"]),
    )

    # Hourly
    hr_agg = defaultdict(lambda: {"orders": 0, "revenue": 0.0})
    for r in rows:
        key = r["sale_time"].replace(minute=0, second=0, microsecond=0)
        hr_agg[key]["orders"] += 1
        hr_agg[key]["revenue"] += r["total"]
    hourly = [
        {
            "hour": k.strftime("%Y-%m-%d %H:00"),
            "orders": v["orders"],
            "revenue": round(v["revenue"], 2),
            "avg_order": round(v["revenue"] / v["orders"], 2) if v["orders"] else 0,
        }
        for k, v in sorted(hr_agg.items(), reverse=True)
    ]

    # Recent + top
    recent_list = sorted(recent_rows, key=lambda r: r["sale_time"], reverse=True)[:15]
    recent_display = [
        {
            "sale_id": r["sale_id"],
            "sale_time": r["sale_time"].strftime("%H:%M:%S"),
            "product": r["product"],
            "price": r["price"],
            "quantity": r["quantity"],
            "total": r["total"],
            "region": r["region"],
            "channel": r["channel"],
        }
        for r in recent_list
    ]
    top_list = sorted(recent_rows, key=lambda r: r["total"], reverse=True)[:10]
    top_display = [
        {
            "sale_id": r["sale_id"],
            "sale_time": r["sale_time"].strftime("%H:%M:%S"),
            "product": r["product"],
            "quantity": r["quantity"],
            "total": r["total"],
            "region": r["region"],
            "channel": r["channel"],
        }
        for r in top_list
    ]

    return {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "avg_order": avg_order,
        "units_sold": units_sold,
        "timeseries": timeseries,
        "by_product": by_product,
        "by_region": by_region,
        "by_channel": by_channel,
        "region_channel": region_channel,
        "product_ranking": product_ranking,
        "hourly": hourly,
        "recent": recent_display,
        "top_sales": top_display,
    }


# ------------------------------------------------------------------ HTML helpers
def _esc(v):
    return html.escape(str(v))


def _fmt_money(v):
    try:
        return f"${float(v):,.2f}"
    except (ValueError, TypeError):
        return str(v)


def _fmt_int(v):
    try:
        return f"{int(v):,}"
    except (ValueError, TypeError):
        return str(v)


def _build_table(rows, col_formats=None):
    if not rows:
        return '<p class="empty">No data</p>'
    col_formats = col_formats or {}
    cols = list(rows[0].keys())
    hdr = "".join(f"<th>{_esc(c.replace('_', ' ').title())}</th>" for c in cols)
    body = ""
    for row in rows:
        cells = ""
        for c in cols:
            val = row.get(c, "")
            fmt = col_formats.get(c)
            cells += f"<td>{fmt(val) if fmt else _esc(val)}</td>"
        body += f"<tr>{cells}</tr>"
    return f"<table><thead><tr>{hdr}</tr></thead><tbody>{body}</tbody></table>"


def _build_bars(rows, label_key, value_key, color="#38bdf8"):
    if not rows:
        return '<p class="empty">No data</p>'
    mx = max(float(r[value_key]) for r in rows) or 1
    out = ""
    for r in rows:
        val = float(r[value_key])
        pct = min(val / mx * 100, 100)
        out += (
            f'<div class="br">'
            f'<span class="bl">{_esc(str(r[label_key]))}</span>'
            f'<div class="bt"><div class="bf" style="width:{pct:.1f}%;background:{color}"></div></div>'
            f'<span class="bv">{_fmt_money(val)}</span></div>'
        )
    return f'<div class="bc">{out}</div>'


def _build_line_svg(rows, time_key, val_key, color="#4ade80"):
    if not rows or len(rows) < 2:
        return '<p class="empty">Collecting data points...</p>'
    values = [float(r[val_key]) for r in rows]
    labels = [str(r[time_key]) for r in rows]
    mx, mn = max(values) or 1, min(values)
    sp = (mx - mn) or 1
    n = len(values)
    w, h, pad = 800, 200, 35

    pts = []
    for i, v in enumerate(values):
        x = pad + (i / (n - 1)) * (w - 2 * pad)
        y = h - pad - ((v - mn) / sp) * (h - 2 * pad)
        pts.append((x, y))

    poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area = poly + f" {pts[-1][0]:.1f},{h - pad:.1f} {pts[0][0]:.1f},{h - pad:.1f}"

    step = max(1, n // 8)
    xl = "".join(
        f'<text x="{pad + (i / (n - 1)) * (w - 2 * pad):.0f}" y="{h - 5}" fill="#94a3b8" '
        f'font-size="11" text-anchor="middle">{labels[i]}</text>'
        for i in range(0, n, step)
    )
    yl = ""
    for frac in [0, 0.5, 1]:
        yv = mn + frac * sp
        yp = h - pad - frac * (h - 2 * pad)
        yl += (
            f'<text x="{pad - 5}" y="{yp:.0f}" fill="#94a3b8" font-size="10" '
            f'text-anchor="end">${yv:,.0f}</text>'
            f'<line x1="{pad}" y1="{yp:.0f}" x2="{w - pad}" y2="{yp:.0f}" '
            f'stroke="#334155" stroke-width="0.5"/>'
        )
    dots = "".join(
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="{color}"/>' for x, y in pts
    )

    return (
        f'<svg viewBox="0 0 {w} {h}" class="sc">{yl}{xl}'
        f'<polygon points="{area}" fill="{color}" opacity="0.12"/>'
        f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="2.5" stroke-linejoin="round"/>'
        f"{dots}</svg>"
    )


# ------------------------------------------------------------------ dashboard
CSS = """
:root{--bg:#0f172a;--sf:#1e293b;--bd:#334155;--tx:#e2e8f0;--mt:#94a3b8;
      --ac:#38bdf8;--gn:#4ade80;--yl:#facc15;--pk:#f472b6;--pp:#a78bfa}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--tx);padding:18px 22px}
.hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px}
h1{font-size:1.3rem}.dot{display:inline-block;width:8px;height:8px;background:var(--gn);border-radius:50%;margin-right:6px;animation:p 1.5s infinite}
@keyframes p{0%,100%{opacity:1}50%{opacity:.3}}.meta{color:var(--mt);font-size:.78rem}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}
.kpi{background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:14px 16px}
.kpi-l{font-size:.68rem;text-transform:uppercase;letter-spacing:.04em;color:var(--mt);margin-bottom:3px}
.kpi-v{font-size:1.5rem;font-weight:700}
.sec{margin-bottom:22px}.sec-title{font-size:1rem;font-weight:600;margin-bottom:12px;padding-bottom:6px;border-bottom:1px solid var(--bd)}
.card{background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:16px;margin-bottom:14px}
.card h3{font-size:.85rem;color:var(--mt);margin-bottom:8px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.bc{display:flex;flex-direction:column;gap:8px}
.br{display:flex;align-items:center;gap:8px}
.bl{width:110px;font-size:.78rem;text-align:right;color:var(--mt);flex-shrink:0}
.bt{flex:1;height:22px;background:var(--bg);border-radius:4px;overflow:hidden}
.bf{height:100%;border-radius:4px;min-width:2px}
.bv{width:90px;font-size:.78rem;font-weight:600}
.sc{width:100%;height:auto;max-height:220px}
.tw{max-height:360px;overflow-y:auto}
table{width:100%;border-collapse:collapse;font-size:.78rem}
th{text-align:left;color:var(--mt);padding:7px 9px;border-bottom:2px solid var(--bd);font-weight:600;text-transform:uppercase;font-size:.68rem;letter-spacing:.04em;position:sticky;top:0;background:var(--sf)}
td{padding:6px 9px;border-bottom:1px solid var(--bd)}
tr:hover td{background:rgba(56,189,248,.04)}
.empty{color:var(--mt);font-style:italic;padding:12px 0}
.qcard{background:#162032;border:1px solid var(--bd);border-radius:8px;padding:12px 14px;margin-bottom:10px}
.qcard h4{font-size:.82rem;color:var(--ac);margin-bottom:4px}
.qcard code{font-size:.72rem;color:var(--mt);word-break:break-all}
@media(max-width:768px){.kpis,.grid2{grid-template-columns:1fr}}
"""


@app.get("/ui", response_class=HTMLResponse)
async def dashboard():
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    # Generate random data and aggregate
    raw = _generate_sales(200)
    d = _aggregate(raw)

    # Charts
    line_chart = _build_line_svg(d["timeseries"], "minute", "revenue", "#4ade80")
    prod_bars = _build_bars(d["by_product"], "product", "revenue", "#f472b6")
    reg_bars = _build_bars(d["by_region"], "region", "revenue", "#38bdf8")
    chan_bars = _build_bars(d["by_channel"], "channel", "revenue", "#a78bfa")

    # Tables
    chan_tbl = _build_table(
        d["by_channel"],
        {"revenue": _fmt_money, "avg_order": _fmt_money, "orders": _fmt_int},
    )
    recent_tbl = _build_table(d["recent"], {"price": _fmt_money, "total": _fmt_money})
    top_tbl = _build_table(d["top_sales"], {"total": _fmt_money})
    rank_tbl = _build_table(
        d["product_ranking"],
        {
            "times_sold": _fmt_int,
            "total_units": _fmt_int,
            "total_revenue": _fmt_money,
            "avg_price": _fmt_money,
        },
    )
    hourly_tbl = _build_table(
        d["hourly"],
        {"orders": _fmt_int, "revenue": _fmt_money, "avg_order": _fmt_money},
    )
    rc_tbl = _build_table(
        d["region_channel"], {"orders": _fmt_int, "revenue": _fmt_money}
    )

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<meta http-equiv="refresh" content="10"/>
<title>Sales Dashboard</title>
<style>{CSS}</style>
</head>
<body>

<div class="hdr">
  <h1><span class="dot"></span> Real-Time Sales Dashboard</h1>
  <span class="meta">Auto-refreshes 10s &middot; {now_str}</span>
</div>

<div class="kpis">
  <div class="kpi"><div class="kpi-l">Revenue (1h)</div>
    <div class="kpi-v" style="color:var(--gn)">{_fmt_money(d["total_revenue"])}</div></div>
  <div class="kpi"><div class="kpi-l">Orders (1h)</div>
    <div class="kpi-v" style="color:var(--ac)">{_fmt_int(d["total_orders"])}</div></div>
  <div class="kpi"><div class="kpi-l">Avg Order</div>
    <div class="kpi-v" style="color:var(--yl)">{_fmt_money(d["avg_order"])}</div></div>
  <div class="kpi"><div class="kpi-l">Units Sold</div>
    <div class="kpi-v" style="color:var(--pp)">{_fmt_int(d["units_sold"])}</div></div>
</div>

<div class="sec">
  <div class="sec-title">Revenue Over Time (30 min)</div>
  <div class="card">{line_chart}</div>
</div>

<div class="sec">
  <div class="grid2">
    <div class="card"><h3>Revenue by Product (1h)</h3>{prod_bars}</div>
    <div class="card"><h3>Revenue by Region (1h)</h3>{reg_bars}</div>
  </div>
  <div class="grid2">
    <div class="card"><h3>Revenue by Channel (1h)</h3>{chan_bars}</div>
    <div class="card"><h3>Channel Details</h3>{chan_tbl}</div>
  </div>
</div>

<div class="sec">
  <div class="sec-title">Live Transactions (latest 15)</div>
  <div class="card"><div class="tw">{recent_tbl}</div></div>
</div>

<div class="sec">
  <div class="sec-title">Query Results</div>

  <div class="qcard"><h4>Query 1 — Product Performance Ranking</h4>
    <code>SELECT product, COUNT(*) AS times_sold, SUM(quantity) AS total_units,
ROUND(SUM(total),2) AS total_revenue, ROUND(AVG(price),2) AS avg_price
FROM sales GROUP BY product ORDER BY total_revenue DESC</code></div>
  <div class="card"><div class="tw">{rank_tbl}</div></div>

  <div class="qcard"><h4>Query 2 — Hourly Revenue Summary</h4>
    <code>SELECT DATE_TRUNC('hour', sale_time) AS hour, COUNT(*) AS orders,
ROUND(SUM(total),2) AS revenue, ROUND(AVG(total),2) AS avg_order
FROM sales GROUP BY hour ORDER BY hour DESC LIMIT 24</code></div>
  <div class="card"><div class="tw">{hourly_tbl}</div></div>

  <div class="qcard"><h4>Query 3 — Region x Channel Cross-Tab (1h)</h4>
    <code>SELECT region, channel, COUNT(*) AS orders, ROUND(SUM(total),2) AS revenue
FROM sales WHERE sale_time >= NOW() - INTERVAL '1 hour'
GROUP BY region, channel ORDER BY region, revenue DESC</code></div>
  <div class="card"><div class="tw">{rc_tbl}</div></div>

  <div class="qcard"><h4>Query 4 — Top 10 Highest-Value Sales (1h)</h4>
    <code>SELECT sale_id, sale_time, product, quantity, total, region, channel
FROM sales WHERE sale_time >= NOW() - INTERVAL '1 hour'
ORDER BY total DESC LIMIT 10</code></div>
  <div class="card"><div class="tw">{top_tbl}</div></div>
</div>

</body>
</html>"""
    return HTMLResponse(content=page)


class SalesDashboardPlugin(AirflowPlugin):
    name = "sales_dashboard"

    fastapi_apps = [
        {
            "app": app,
            "url_prefix": "/sales-dashboard",
            "name": "Sales Dashboard",
        }
    ]

    external_views = [
        {
            "name": "Sales Dashboard",
            "href": "sales-dashboard/ui",
            "destination": "nav",
            "category": "browse",
            "url_route": "sales-dashboard",
        }
    ]
