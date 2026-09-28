"""Reproducible case study for the article: VPIN, taker imbalance and Polymarket repricing
around the largest 15-minute BTCUSDT drop between 2026-07-28 and 2026-09-26.

Data: Binance public 1m klines (exact taker-buy / taker-sell notional per bar) and
Polymarket's public CLOB price history. No API keys needed:

    pip install httpx && python research/vpin_case_study.py
"""

from __future__ import annotations

import bisect
import calendar
import json
import statistics
import time
from collections import deque
from typing import List, Sequence, Tuple

import httpx

BINANCE_KLINES = "https://data-api.binance.vision/api/v3/klines"
GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"
CLOB_HISTORY = "https://clob.polymarket.com/prices-history"

START = calendar.timegm((2026, 7, 28, 0, 0, 0)) * 1000
END = calendar.timegm((2026, 9, 26, 0, 0, 0)) * 1000
WARMUP_BARS = 7 * 1440
N_BUCKETS = 50
FAST_BUCKET_USD = 1_000_000.0

Series = List[Tuple[int, float]]


def utc(ms: int, fmt: str = "%Y-%m-%d %H:%M") -> str:
    return time.strftime(fmt, time.gmtime(ms / 1000))


def fetch_klines(client: httpx.Client, symbol: str, start_ms: int, end_ms: int) -> list:
    rows, t = [], start_ms
    while t < end_ms:
        params = {"symbol": symbol, "interval": "1m", "startTime": t, "endTime": end_ms - 1, "limit": 1000}
        batch = client.get(BINANCE_KLINES, params=params).raise_for_status().json()
        if not batch:
            break
        rows += batch
        t = batch[-1][0] + 60_000
    return rows


def vpin_series(bars: Sequence[list], bucket_usd: float, n: int = N_BUCKETS) -> Series:
    """VPIN = sum(|V_buy - V_sell|) / (n * V) over the last n completed volume buckets.

    Each 1m bar's taker-buy and taker-sell notional is poured into fixed-notional buckets;
    a bar that overflows a bucket is split pro rata. Stamped with the bar's open time.
    """
    buy = sell = 0.0
    imbalances: deque = deque(maxlen=n)
    out: Series = []
    for bar in bars:
        q, qb = float(bar[7]), float(bar[10])
        qs = q - qb
        while q > 1e-9:
            frac = min(bucket_usd - (buy + sell), q) / q
            buy, sell = buy + qb * frac, sell + qs * frac
            qb, qs, q = qb * (1 - frac), qs * (1 - frac), q * (1 - frac)
            if buy + sell >= bucket_usd - 1e-6:
                imbalances.append(abs(buy - sell))
                buy = sell = 0.0
                if len(imbalances) == n:
                    out.append((bar[0], sum(imbalances) / (n * bucket_usd)))
    return out


def value_at(series: Series, t_ms: int) -> Tuple[float, float]:
    """(VPIN as of t_ms, its percentile within the trailing 7 days)."""
    ts = [t for t, _ in series]
    j = bisect.bisect_right(ts, t_ms) - 1
    k0 = bisect.bisect_left(ts, t_ms - 7 * 86_400_000)
    history = sorted(v for _, v in series[k0:j])
    return series[j][1], bisect.bisect_left(history, series[j][1]) / len(history)


def conditional_lift(series: Series, times: List[int], closes: List[float]) -> str:
    """P(next-15m |move| >= global p99 | VPIN >= its p90) vs the same probability otherwise."""
    ts = [t for t, _ in series]
    p90 = statistics.quantiles([v for _, v in series], n=10)[-1]
    hi, lo = [], []
    for i in range(WARMUP_BARS, len(closes) - 15, 15):
        j = bisect.bisect_right(ts, times[i]) - 1
        if j >= 0:
            (hi if series[j][1] >= p90 else lo).append(abs(closes[i + 15] / closes[i] - 1))
    big = statistics.quantiles(hi + lo, n=100)[98]
    p_hi = sum(m >= big for m in hi) / len(hi)
    p_lo = sum(m >= big for m in lo) / len(lo)
    return (
        f"P(|move| >= {big * 100:.2f}% | VPIN >= p90) = {p_hi:.2%} (n={len(hi)}) vs "
        f"{p_lo:.2%} (n={len(lo)}) otherwise -> lift {p_hi / p_lo:.1f}x"
    )


def polymarket_history(client: httpx.Client, event_slug: str, start_s: int, end_s: int) -> dict:
    event = client.get(GAMMA_EVENTS, params={"slug": event_slug}).raise_for_status().json()[0]
    yes_token = json.loads(event["markets"][0]["clobTokenIds"])[0]
    params = {"market": yes_token, "startTs": start_s, "endTs": end_s, "fidelity": 1}
    history = client.get(CLOB_HISTORY, params=params).raise_for_status().json()["history"]
    return {p["t"] // 60 * 60_000: p["p"] for p in history}


def main() -> None:
    with httpx.Client(timeout=20) as client:
        bars = fetch_klines(client, "BTCUSDT", START, END)
        times = [b[0] for b in bars]
        closes = [float(b[4]) for b in bars]
        days = (END - START) / 86_400_000
        adv = sum(float(b[7]) for b in bars) / days
        slow = vpin_series(bars, adv / N_BUCKETS)
        fast = vpin_series(bars, FAST_BUCKET_USD)
        print(f"{len(bars)} bars, {utc(START)} -> {utc(END)} UTC, ADV ${adv / 1e9:.2f}B")
        for name, s, v in (("slow V=ADV/50", slow, adv / N_BUCKETS), ("fast V=$1M", fast, FAST_BUCKET_USD)):
            q = statistics.quantiles([x for _, x in s], n=20)
            print(f"  VPIN[{name}, ${v / 1e6:.1f}M] p5={q[0]:.3f} median={q[9]:.3f} p95={q[-1]:.3f}")

        ret, i = min((closes[k] / closes[k - 15] - 1, k) for k in range(WARMUP_BARS, len(closes)))
        s = i - 15
        print(f"\nLargest 15m drop: {ret * 100:+.2f}%, {utc(times[s])} -> {utc(times[i], '%H:%M')} UTC "
              f"({closes[s]:,.0f} -> {closes[i]:,.0f}, low {min(float(b[3]) for b in bars[s:i + 2]):,.0f})")
        for name, series in (("slow", slow), ("fast", fast)):
            v, cdf = value_at(series, times[s])
            print(f"  VPIN[{name}] at window start = {v:.3f} (trailing-7d percentile {cdf:.0%})")

        # Polymarket's 15m "BTC Up or Down" market whose window contains the bottom of the move.
        window_start = times[i] // 900_000 * 900
        pm = polymarket_history(client, f"btc-updown-15m-{window_start}", window_start - 900, window_start + 1200)
        vol15 = [sum(float(b[7]) for b in bars[k - 15:k]) for k in range(s - 300, s + 1, 15)]
        med = statistics.median(vol15)
        mad = statistics.median(abs(x - med) for x in vol15)

        print(f"\n{'UTC':<6}{'close':>9}{'notional':>10}{'taker_buy':>10}{'vpin_fast':>10}{'pctl':>6}"
              f"{'vpin_slow':>10}{'PM Up':>7}")
        for k in range(s, i + 3):
            b = bars[k]
            vf, cf = value_at(fast, b[0])
            vs, _ = value_at(slow, b[0])
            up = pm.get(b[0])
            print(f"{utc(b[0], '%H:%M'):<6}{float(b[4]):>9,.0f}{float(b[7]) / 1e6:>9.1f}M{float(b[10]) / float(b[7]):>10.2f}"
                  f"{vf:>10.3f}{cf:>6.0%}{vs:>10.3f}{up if up is not None else float('nan'):>7.3f}")
        move_usd = sum(float(b[7]) for b in bars[s:i + 1])
        print(f"15m notional ${move_usd / 1e6:.0f}M, robust volume z-score {(move_usd - med) * 0.6745 / mad:.1f}")

        print("\nConditional backtest (non-overlapping 15m windows after a 7-day warm-up):")
        print(f"  slow: {conditional_lift(slow, times, closes)}")
        print(f"  fast: {conditional_lift(fast, times, closes)}")


if __name__ == "__main__":
    main()
