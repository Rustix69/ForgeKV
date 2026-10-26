#!/usr/bin/env python3
"""Draw benchmark charts from the CSV exports in bench-results/csv/.

Writes SVG charts to docs/images/ (embedded in README.md). With --png it also
renders each SVG to a 2x PNG with a headless Chrome/Chromium found on PATH
(for sharing outside GitHub). Standard library only; regenerate the CSVs
first with scripts/bench_csv.py.

Usage: scripts/bench_charts.py [--png]

Visual rules: one series per chart in a single accent hue (emphasis charts
add one muted gray), values labelled directly, hairline solid gridlines,
log axes drawn as dot plots (bar length would misstate ratios), and two
measures of different scale drawn as two panels, never one dual axis.
"""

import csv
import html
import math
import os
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(ROOT, "bench-results", "csv")
OUT = os.path.join(ROOT, "docs", "images")
CURRENT = "20261026T173115Z"   # Phase 8 session, crc32:bbce3c06
HISTORICAL = "20261026T160449Z"  # Phase 7 session, crc32:0f6f6c76

# Palette (light surface): slot-1 accent, its lighter step for ranges, a
# muted gray for de-emphasis, and the text/chrome tokens.
SURFACE = "#fcfcfb"
ACCENT = "#2a78d6"
ACCENT_LIGHT = "#86b6ef"
DEEMPH = "#898781"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
FONT = 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif'

WIDTH = 880
LABEL_W = 300          # left column for row labels
RIGHT_PAD = 150        # room for value labels after the longest mark
ROW_H = 46
BAR = 22               # bar thickness (<= 24px)


def rows(name):
    with open(os.path.join(CSV_DIR, name), newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def by_name(session, kind="load-summary"):
    return {r["name"]: r for r in rows(f"{session}-{kind}.csv")}


def esc(s):
    return html.escape(str(s), quote=True)


def fmt_int(v):
    return f"{round(float(v)):,}"


def fmt_ns(ns):
    ns = float(ns)
    if ns < 1e3:
        return f"{ns:.0f} ns"
    if ns < 1e6:
        return f"{ns / 1e3:.1f} µs"
    if ns < 1e9:
        return f"{ns / 1e6:.1f} ms"
    return f"{ns / 1e9:.2f} s"


def compact(v):
    v = float(v)
    for div, suf in ((1e6, "M"), (1e3, "K")):
        if v >= div:
            x = v / div
            return f"{x:.0f}{suf}" if x == int(x) else f"{x:.1f}{suf}"
    return f"{v:.0f}"


class Svg:
    def __init__(self, title, desc):
        self.parts = []
        self.title, self.desc = title, desc
        self.y = 0

    def text(self, x, y, s, size=13, fill=INK, weight=400, anchor="start"):
        self.parts.append(
            f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{fill}" '
            f'font-weight="{weight}" text-anchor="{anchor}">{esc(s)}</text>')

    def line(self, x1, y1, x2, y2, stroke, width=1):
        self.parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'stroke="{stroke}" stroke-width="{width}" stroke-linecap="round"/>')

    def hbar(self, x0, x1, yc, fill):
        """Bar from the baseline x0 to x1: square at the baseline, 4px rounded end."""
        h, r = BAR, 4
        y = yc - h / 2
        w = max(x1 - x0, r)
        self.parts.append(
            f'<path d="M{x0:.1f},{y:.1f} h{w - r:.1f} a{r},{r} 0 0 1 {r},{r} '
            f'v{h - 2 * r} a{r},{r} 0 0 1 -{r},{r} h-{w - r:.1f} z" fill="{fill}"/>')

    def dot(self, x, y, fill):
        # >= 8px marker with a 2px ring in the surface colour.
        self.parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="{fill}" '
            f'stroke="{SURFACE}" stroke-width="2"/>')

    def header(self, title, subtitle_lines):
        self.text(24, 36, title, size=19, weight=650)
        y = 60
        for line in subtitle_lines:
            self.text(24, y, line, size=13, fill=INK2)
            y += 19
        self.y = y + 8

    def footer(self, lines):
        y = self.y + 6
        for line in lines:
            self.text(24, y, line, size=11.5, fill=MUTED)
            y += 16
        self.y = y + 8

    def render(self):
        h = math.ceil(self.y)
        return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{h}" '
                f'viewBox="0 0 {WIDTH} {h}" font-family=\'{FONT}\' role="img">\n'
                f'<title>{esc(self.title)}</title>\n<desc>{esc(self.desc)}</desc>\n'
                f'<rect width="{WIDTH}" height="{h}" fill="{SURFACE}"/>\n'
                + "\n".join(self.parts) + "\n</svg>\n")


def panel(svg, items, scale, ticks, tick_fmt, axis_title, legend=None):
    """One horizontal panel. items: dicts with label, value, text, color and
    optional lo/hi (range). scale: ('log', lo, hi) or ('linear', 0, hi)."""
    kind, dmin, dmax = scale
    x0, x1 = LABEL_W, WIDTH - RIGHT_PAD

    def X(v):
        v = max(float(v), dmin)
        if kind == "log":
            return x0 + (math.log10(v) - math.log10(dmin)) / (math.log10(dmax) - math.log10(dmin)) * (x1 - x0)
        return x0 + (v - dmin) / (dmax - dmin) * (x1 - x0)

    if legend:
        lx = x0
        for name, color in legend:
            svg.parts.append(f'<rect x="{lx}" y="{svg.y - 10}" width="12" height="12" rx="3" fill="{color}"/>')
            svg.text(lx + 18, svg.y, name, size=12.5, fill=INK2)
            lx += 30 + 7.2 * len(name)
        svg.y += 18

    top = svg.y + 6
    bottom = top + ROW_H * len(items)
    for t in ticks:  # gridlines: solid hairlines
        svg.line(X(t), top, X(t), bottom, GRID)
    for i, it in enumerate(items):
        yc = top + ROW_H * i + ROW_H / 2
        label_lines = it["label"] if isinstance(it["label"], list) else [it["label"]]
        ly = yc - 7 * (len(label_lines) - 1) + 4.5
        for j, ln in enumerate(label_lines):
            svg.text(x0 - 14, ly + 15 * j, ln, size=13 if j == 0 else 11.5,
                     fill=INK if j == 0 else MUTED, anchor="end")
        if kind == "log":
            if "lo" in it:
                svg.line(X(it["lo"]), yc, X(it["hi"]), yc, ACCENT_LIGHT if it["color"] == ACCENT else AXIS, 2)
            svg.dot(X(it["value"]), yc, it["color"])
            end = max(X(it.get("hi", it["value"])), X(it["value"]) + 6)
        else:
            if float(it["value"]) > 0:  # a zero draws no bar, only its label
                svg.hbar(x0, X(it["value"]), yc, it["color"])
            end = X(it["value"])
        svg.text(end + 10, yc + 4.5, it["text"], size=13, weight=600)
    svg.line(x0, top, x0, bottom, AXIS)  # baseline
    for t in ticks:
        svg.text(X(t), bottom + 18, tick_fmt(t), size=11.5, fill=MUTED, anchor="middle")
    svg.text((x0 + x1) / 2, bottom + 38, axis_title, size=12, fill=INK2, anchor="middle")
    svg.y = bottom + 56


def save(name, svg):
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name + ".svg")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(svg.render())
    return path, math.ceil(svg.y)


def chart_durability(cur):
    probe = cur["fsync-probe-start"]
    always = cur["engine-fillrandom-always-c1"]
    never = cur["engine-fillrandom-never-c1"]
    ratio = float(never["ops_per_sec_median"]) / float(always["ops_per_sec_median"])
    items = []
    for label, r in [(["Raw fsync, no database", "append 100 B + fdatasync"], probe),
                     (["ForgeKV, SyncMode::Always", "WAL fsync before every ack"], always),
                     (["ForgeKV, SyncMode::Never", "acked once in the OS page cache"], never)]:
        items.append(dict(label=label, value=r["ops_per_sec_median"],
                          lo=r["ops_per_sec_min"], hi=r["ops_per_sec_max"],
                          text=f'{fmt_int(r["ops_per_sec_median"])}/s', color=ACCENT))
    svg = Svg("The cost of durability",
              "Single-thread write throughput: raw fsync, ForgeKV with fsync per write, ForgeKV without fsync.")
    svg.header("The cost of durability: one writer, same disk",
               [f"Skipping fsync gives about {round(ratio, -1):,.0f}× the write throughput. With fsync on, ForgeKV runs",
                "close to the disk's raw sync rate: the fsync, not the engine, sets the limit."])
    panel(svg, items, ("log", 100, 1_000_000), [100, 1_000, 10_000, 100_000, 1_000_000],
          compact, "writes per second (log scale)")
    svg.footer(["Dot = median of 5 runs, line = min–max. Random 16 B keys, 100 B values, 1 thread, 10 s measured.",
                "One laptop (i5-12500H, ext4 on NVMe). Session 20261026T173115Z, code crc32:bbce3c06."])
    return save("durability-cost", svg)


def chart_fairness(cur, old):
    key_lat, key_tp = "engine-fillrandom-always-c4", "engine-fillrandom-never-c4"
    svg = Svg("Fixing writer starvation",
              "Worst single write latency and throughput with 4 writer threads, before and after the FIFO writer lock.")
    svg.header("Fixing writer starvation with a FIFO writer lock",
               ["With std::sync::Mutex one writer could wait 10 seconds while others re-took the lock.",
                "A first-come-first-served lock bounds the wait; the cost is lower throughput without fsync."])
    before, after = (DEEMPH, "Before: std::sync::Mutex"), (ACCENT, "After: FIFO writer lock")
    legend = [(before[1], before[0]), (after[1], after[0])]
    svg.text(24, svg.y + 6, "Worst single write, 4 writers, SyncMode::Always", size=14, weight=600)
    svg.y += 26
    lat = [dict(label=["Before", "std::sync::Mutex"], value=old[key_lat]["worst_max_ns"],
                text=fmt_ns(old[key_lat]["worst_max_ns"]), color=DEEMPH),
           dict(label=["After", "FIFO writer lock"], value=cur[key_lat]["worst_max_ns"],
                text=fmt_ns(cur[key_lat]["worst_max_ns"]), color=ACCENT)]
    panel(svg, lat, ("log", 1e6, 1e11), [1e6, 1e7, 1e8, 1e9, 1e10, 1e11],
          lambda t: fmt_ns(t).replace(".0 ", " ").replace(".00 ", " "),
          "worst single write latency across 5 runs (log scale)", legend=legend)
    svg.text(24, svg.y + 6, "Throughput, 4 writers, SyncMode::Never (no fsync)", size=14, weight=600)
    svg.y += 26
    tp = [dict(label=["Before", "std::sync::Mutex"], value=old[key_tp]["ops_per_sec_median"],
               text=f'{fmt_int(old[key_tp]["ops_per_sec_median"])}/s', color=DEEMPH),
          dict(label=["After", "FIFO writer lock"], value=cur[key_tp]["ops_per_sec_median"],
               text=f'{fmt_int(cur[key_tp]["ops_per_sec_median"])}/s', color=ACCENT)]
    panel(svg, tp, ("linear", 0, 400_000), [0, 100_000, 200_000, 300_000, 400_000],
          compact, "writes per second, median of 5 runs")
    svg.footer(["Before = session 20261026T160449Z (code crc32:0f6f6c76); after = session 20261026T173115Z",
                "(code crc32:bbce3c06). Same laptop and workloads, different sessions. Random keys, 100 B values."])
    return save("writer-fairness", svg)


def chart_stalls(cur, cur_runs):
    def share(name):
        rs = [r for r in cur_runs if r["name"] == name]
        vals = sorted(float(r["end_write_stall_secs"]) / float(r["elapsed_secs"]) for r in rs)
        n = len(vals)
        return 100 * (vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2)
    cfgs = [("engine-fillrandom-never-c4-v1000", ["Random writes, 4 threads", "1000 B values"]),
            ("engine-fillrandom-never-c1", ["Random writes, 1 thread", "100 B values"]),
            ("engine-fillseq-never-c1", ["Sequential writes, 1 thread", "100 B values"]),
            ("engine-readwrite50-never-c4", ["50% reads / 50% writes", "4 threads, 100 B values"]),
            ("engine-fillrandom-never-c4", ["Random writes, 4 threads", "100 B values"])]
    items = []
    for name, label in cfgs:
        r = cur[name]
        pct = share(name)
        stalls = float(r["write_stalls_median"])
        during = float(r["write_stalls_during_compaction_median"])
        note = f"{pct:.0f}%" + (f"  ({during:.0f} of {stalls:.0f} stalls during compaction)" if stalls else "")
        items.append(dict(label=label, value=pct, text=note, color=ACCENT))
    svg = Svg("Compaction stalls",
              "Share of the measured time during which writes were blocked waiting for memtable room.")
    svg.header("Compaction stalls: how long writes were blocked",
               ["Writers wait when both memtables are full and the single background worker is still busy.",
                "Measured with the engine's stall counters. In write-heavy runs, nearly every stall began during a compaction."])
    global RIGHT_PAD
    saved, RIGHT_PAD = RIGHT_PAD, 290
    panel(svg, items, ("linear", 0, 100), [0, 25, 50, 75, 100], lambda t: f"{t:.0f}%",
          "share of the measured phase with writes blocked (median of 5 runs)")
    RIGHT_PAD = saved
    svg.footer(["SyncMode::Never, 200,000 keys. Two-level compaction with one background worker.",
                "Session 20261026T173115Z, code crc32:bbce3c06."])
    return save("compaction-stalls", svg)


def chart_reads(cur):
    items = []
    for name, label in [("engine-readrandom-always-c1", "1 thread"),
                        ("engine-readrandom-always-c4", "4 threads"),
                        ("engine-readrandom-always-c8", "8 threads")]:
        r = cur[name]
        items.append(dict(label=[label, f'GET p99 {fmt_ns(r["get_p99_ns_median"])}'],
                          value=r["ops_per_sec_median"],
                          text=f'{fmt_int(r["ops_per_sec_median"])}/s', color=ACCENT))
    svg = Svg("Read scaling", "Random-read throughput with 1, 4 and 8 threads.")
    svg.header("Reads scale with threads",
               ["Readers take a snapshot and read without holding any engine lock.",
                "The dataset fits in RAM, so these reads come from the OS page cache, not the disk."])
    panel(svg, items, ("linear", 0, 3_000_000), [0, 1_000_000, 2_000_000, 3_000_000],
          compact, "reads per second, median of 5 runs")
    svg.footer(["Random GETs over 200,000 preloaded keys (16 B keys, 100 B values), 10 s measured.",
                "Session 20261026T173115Z, code crc32:bbce3c06."])
    return save("read-scaling", svg)


def chart_server(cur):
    items = []
    for name, label in [("server-readwrite50-never-c1", "1 client"),
                        ("server-readwrite50-never-c8", "8 clients"),
                        ("server-readwrite50-never-c32", "32 clients")]:
        r = cur[name]
        items.append(dict(label=[label, f'GET p50 {fmt_ns(r["get_p50_ns_median"])}'],
                          value=r["ops_per_sec_median"],
                          text=f'{fmt_int(r["ops_per_sec_median"])}/s', color=ACCENT))
    svg = Svg("Server throughput", "TCP server throughput with 1, 8 and 32 clients, 50% reads.")
    svg.header("Over TCP: throughput by client count",
               ["Each client sends one request at a time over loopback to the Tokio server.",
                "Throughput peaks at 8 clients; at 32 it falls and median GET latency doubles."])
    panel(svg, items, ("linear", 0, 200_000), [0, 50_000, 100_000, 150_000, 200_000],
          compact, "requests per second, median of 5 runs")
    svg.footer(["50% GET / 50% PUT, SyncMode::Never, 200,000 keys. Clients and server share one machine.",
                "Session 20261026T173115Z, code crc32:bbce3c06."])
    return save("server-scaling", svg)


def to_png(svg_path, height):
    chrome = next((shutil.which(c) for c in ("google-chrome", "chromium", "chromium-browser")
                   if shutil.which(c)), None)
    if not chrome:
        sys.exit("--png needs google-chrome or chromium on PATH")
    png = svg_path[:-4] + ".png"
    subprocess.run([chrome, "--headless", "--disable-gpu", "--hide-scrollbars",
                    "--force-device-scale-factor=2", f"--window-size={WIDTH},{height}",
                    f"--screenshot={png}", "file://" + svg_path],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return png


def main():
    cur, old = by_name(CURRENT), by_name(HISTORICAL)
    cur_runs = rows(f"{CURRENT}-load-runs.csv")
    made = [chart_durability(cur), chart_fairness(cur, old), chart_stalls(cur, cur_runs),
            chart_reads(cur), chart_server(cur)]
    for path, h in made:
        print(os.path.relpath(path, ROOT))
        if "--png" in sys.argv[1:]:
            print(os.path.relpath(to_png(path, h), ROOT))


if __name__ == "__main__":
    main()
