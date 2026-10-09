#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Render a projection-sweep chart (SVG, log-log) from the CSV that
# `footer_decode_bench --sweep` prints. Dependency-free.
#
#   ./footer_decode_bench file.jt.parquet --sweep file.modular \
#       | python3 plot_sweep.py "hits: 105 cols x 226 row groups" > projection_sweep.svg

import html
import math
import sys

title = sys.argv[1] if len(sys.argv) > 1 else "footer decode: projection sweep"

lines = [ln.strip() for ln in sys.stdin if ln.strip()]
rows = [ln.split(",") for ln in lines[1:]]  # skip header
xs = [int(r[0]) for r in rows]
header = lines[0].split(",")  # projected,<name>_us_per_op,...
palette = {"standard": "#dc2626", "walk": "#64748b", "index": "#2563eb", "modular": "#9333ea"}
cols = []
for j, h in enumerate(header[1:], start=1):
    label = h[:-10] if h.endswith("_us_per_op") else h
    color = palette.get(label, "#0891b2")
    disp = "index (jump table)" if label == "index" else label
    cols.append((disp, [float(r[j]) for r in rows], color))
cols = [c for c in cols if any(v > 0 for v in c[1])]  # drop unmeasured (e.g. no modular)

W, H = 900, 560
L, R, T, B = 90, 260, 70, 70          # margins (R leaves room for the legend)
PW, PH = W - L - R, H - T - B

xmin, xmax = min(xs), max(xs)
allv = [v for _, vals, _ in cols for v in vals if v > 0]
ylo = 10 ** math.floor(math.log10(min(allv)))
yhi = 10 ** math.ceil(math.log10(max(allv)))

lxmin, lxmax = math.log10(xmin), math.log10(xmax)
lylo, lyhi = math.log10(ylo), math.log10(yhi)

def px(x):
    return L + (math.log10(x) - lxmin) / (lxmax - lxmin) * PW if lxmax > lxmin else L
def py(v):
    return T + (1 - (math.log10(v) - lylo) / (lyhi - lylo)) * PH

p = []
p.append('<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="{}" viewBox="0 0 {} {}">'.format(W, H, W, H))
p.append('<rect width="100%" height="100%" fill="#ffffff"/>')
p.append('<style>text{font-family:system-ui,-apple-system,sans-serif;fill:#172033}'
         '.title{font-size:20px;font-weight:700}.sub{font-size:13px;fill:#526071}'
         '.ax{font-size:12px;fill:#526071}.leg{font-size:13px;font-weight:600}</style>')
p.append('<text class="title" x="{}" y="30">Footer decode: resolving placement + stats for a projection</text>'.format(L))
p.append('<text class="sub" x="{}" y="50">{}  —  µs/op vs. projected columns (log-log); same info from each layout</text>'.format(L, html.escape(title)))

# y grid (decades) + labels
v = ylo
while v <= yhi + 1e-9:
    y = py(v)
    p.append('<line x1="{}" y1="{:.1f}" x2="{}" y2="{:.1f}" stroke="#e5e9f0"/>'.format(L, y, L + PW, y))
    lab = ("{:g} ms/op".format(v / 1000.0)) if v >= 1000 else ("{:g} µs/op".format(v))
    p.append('<text class="ax" x="{}" y="{:.1f}" text-anchor="end">{}</text>'.format(L - 8, y + 4, lab))
    v *= 10

# x ticks at the sampled column counts
for x in xs:
    xx = px(x)
    p.append('<line x1="{:.1f}" y1="{}" x2="{:.1f}" y2="{}" stroke="#eef1f6"/>'.format(xx, T, xx, T + PH))
    p.append('<text class="ax" x="{:.1f}" y="{}" text-anchor="middle">{}</text>'.format(xx, T + PH + 20, x))
p.append('<text class="ax" x="{:.1f}" y="{}" text-anchor="middle">projected columns (of {})</text>'.format(L + PW / 2, T + PH + 46, xmax))

# axes
p.append('<line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#94a3b8"/>'.format(L, T, L, T + PH))
p.append('<line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#94a3b8"/>'.format(L, T + PH, L + PW, T + PH))

# series
for name, vals, color in cols:
    pts = " ".join("{:.1f},{:.1f}".format(px(x), py(v)) for x, v in zip(xs, vals))
    p.append('<polyline points="{}" fill="none" stroke="{}" stroke-width="2.5"/>'.format(pts, color))
    for x, v in zip(xs, vals):
        p.append('<circle cx="{:.1f}" cy="{:.1f}" r="3.2" fill="{}"/>'.format(px(x), py(v), color))

# legend (right), with the endpoint value
lx = L + PW + 30
ly = T + 10
p.append('<text class="ax" x="{}" y="{}">resolve (µs/op @ {} / {} columns)</text>'.format(lx, ly - 14, xmin, xmax))
for name, vals, color in cols:
    p.append('<line x1="{}" y1="{:.1f}" x2="{}" y2="{:.1f}" stroke="{}" stroke-width="3"/>'.format(lx, ly, lx + 26, ly, color))
    p.append('<circle cx="{}" cy="{:.1f}" r="3.2" fill="{}"/>'.format(lx + 13, ly, color))
    p.append('<text class="leg" x="{}" y="{:.1f}" fill="{}">{}</text>'.format(lx + 34, ly + 4, color, html.escape(name)))
    p.append('<text class="ax" x="{}" y="{:.1f}">{:.0f} → {:.0f}</text>'.format(lx + 34, ly + 20, vals[0], vals[-1]))
    ly += 46

p.append('</svg>')
sys.stdout.write("\n".join(p) + "\n")
