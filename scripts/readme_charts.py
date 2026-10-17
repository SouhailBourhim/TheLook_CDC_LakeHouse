"""Draw the README's charts as SVG, from measured results (docs/results.md).

Plain Python, no plotting library: each chart is a few lines and text, and
SVG keeps them sharp and small. Every chart is written twice, for GitHub's
light and dark themes (the README picks one with <picture>). Colors are the
validated default palette (blue, orange: colorblind-safe in both themes).

The numbers are copied from docs/results.md, where each comes with its run;
drills/logs/ is git-ignored, so the script does not read it.

Usage: python3 scripts/readme_charts.py   (writes docs/images/*.svg)
"""

from pathlib import Path
from xml.sax.saxutils import escape

OUT = Path(__file__).resolve().parent.parent / "docs" / "images"

THEMES = {
    "light": {
        "surface": "#fcfcfb",
        "ink": "#0b0b0b",
        "ink2": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
        "panel": "#f3f2ee",
        "border": "#dcdbd5",
        "series": ["#2a78d6", "#eb6834"],
    },
    "dark": {
        "surface": "#1a1a19",
        "ink": "#f0efec",
        "ink2": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
        "panel": "#232322",
        "border": "#3a3a37",
        "series": ["#3987e5", "#d95926"],
    },
}
FONT = "system-ui, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"

# --- the data (docs/results.md) ------------------------------------------------

# P8 step 11: retained WAL (MB) at each minute with Kafka Connect stopped.
SLOT_WAL = [4.6, 8, 12, 15, 18, 22, 26, 29, 32, 36, 40, 43, 47, 50, 54, 58, 61, 65,
            68, 72, 75, 79, 83, 86, 89, 93, 97, 100, 103, 107, 111, 114, 118, 121,
            125, 128, 132, 135, 139, 142, 145, 149, 153, 156, 160, 164, 167, 170,
            174, 177, 181, 184, 188, 191, 195, 198, 202, 205, 208, 212]  # fmt: skip
SLOT_ALERT_MINUTE = 31  # ReplicationSlotInactive firing (rule: inactive 30 min)

# P8 step 9: bad rows the revenue contract counted after each day's rebuild.
BACKFILL_DAYS = ["Oct 5", "Oct 6", "Oct 7"]
BACKFILL_BUG = [1, 2, 3]
BACKFILL_FIX = [2, 1, 0]

# P8 steps 10-12: committed Kafka records checked against bronze, per drill.
DRILLS = [
    ("Debezium stopped for an hour", 113_570),
    ("Bronze stream killed mid-batch", 18_767),
    ("Connector offsets replayed", 16_625),
    ("Connect killed, Kafka restarted", 11_922),
]


# --- SVG helpers -----------------------------------------------------------------


def svg(width: int, height: int, theme: dict, body: list[str], title: str) -> str:
    title = escape(title, {'"': "&quot;"})  # also an attribute value
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" role="img" aria-label="{title}" '
            f'font-family="{FONT}">',
            f"<title>{title}</title>",
            f'<rect width="{width}" height="{height}" rx="8" fill="{theme["surface"]}"/>',
            *body,
            "</svg>",
        ]
    )


def text(x, y, s, fill, size=12, anchor="start", weight=400) -> str:
    s = escape(str(s))  # "<", "&" in a label would break the XML
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" fill="{fill}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}">{s}</text>'
    )


def line(x1, y1, x2, y2, stroke, width=1) -> str:
    return (
        f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
        f'stroke="{stroke}" stroke-width="{width}"/>'
    )


def dot(x, y, fill, surface) -> str:
    # r 4 (8 px marker) with a 2 px ring in the surface color.
    return (
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{surface}"/>'
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{fill}"/>'
    )


def header(t: dict, title: str, *subtitle: str) -> list[str]:
    """Title, then one subtitle line per argument (a line never wraps in SVG)."""
    return [text(24, 32, title, t["ink"], 16, weight=600)] + [
        text(24, 52 + 17 * i, s, t["ink2"], 12) for i, s in enumerate(subtitle)
    ]


# --- charts ----------------------------------------------------------------------


def slot_chart(t: dict) -> str:
    w, h = 760, 346
    left, right, top, bottom = 64, 708, 104, 300
    ymax = 250

    def x(minute):
        return left + (right - left) * minute / 60

    def y(mb):
        return bottom - (bottom - top) * mb / ymax

    body = header(
        t,
        "Kafka Connect stopped for an hour: PostgreSQL keeps the WAL",
        "Retained WAL of the replication slot while the generator kept writing "
        "(P8 step 11). Cap: 10 GB.",
        "After the restart, Debezium caught up in 44 s: 0 rows lost, 0 duplicated.",
    )
    for mb in (0, 50, 100, 150, 200, 250):
        body.append(line(left, y(mb), right, y(mb), t["grid"] if mb else t["axis"]))
        body.append(text(left - 8, y(mb) + 4, f"{mb}", t["muted"], 11, "end"))
    body.append(text(left - 8, top - 12, "MB", t["muted"], 11, "end"))
    for m in (0, 10, 20, 30, 40, 50, 60):
        body.append(text(x(m), bottom + 18, f"{m} min", t["muted"], 11, "middle"))
    points = [(0, 0.7)] + [(i + 1, v) for i, v in enumerate(SLOT_WAL)]
    path = " ".join(f"{x(m):.1f},{y(v):.1f}" for m, v in points)
    blue = t["series"][0]
    body.append(
        f'<polygon points="{x(0):.1f},{y(0):.1f} {path} {x(60):.1f},{y(0):.1f}" '
        f'fill="{blue}" fill-opacity="0.10"/>'
    )
    body.append(
        f'<polyline points="{path}" fill="none" stroke="{blue}" stroke-width="2" '
        'stroke-linejoin="round" stroke-linecap="round"/>'
    )
    # The alert: a hairline at minute 31, labelled in text tokens.
    ax = x(SLOT_ALERT_MINUTE)
    body.append(line(ax, top, ax, bottom, t["ink2"]))
    body.append(
        text(
            ax - 6, top + 14, "ReplicationSlotInactive fires,", t["ink"], 12, "end", 600
        )
    )
    body.append(
        text(ax - 6, top + 30, "minute 31: email received", t["ink2"], 12, "end")
    )
    end = SLOT_WAL[-1]
    body.append(dot(x(60), y(end), blue, t["surface"]))
    body.append(
        text(x(60) - 10, y(end) - 10, f"{end:.0f} MB", t["ink"], 12, "end", 600)
    )
    return svg(
        w, h, t, body, "Retained WAL while Kafka Connect was stopped for an hour"
    )


def backfill_chart(t: dict) -> str:
    w, h = 760, 330
    left, right, top, bottom = 48, 470, 100, 278
    ymax = 3

    def x(i):
        return left + 40 + (right - left - 80) * i / 2

    def y(v):
        return bottom - (bottom - top) * v / ymax

    body = header(
        t,
        "A backfill's quality check sees the whole table",
        "Bad rows the revenue contract counted after each day was rebuilt (P8 step 9). "
        "Days run oldest first.",
    )
    for v in range(ymax + 1):
        body.append(line(left, y(v), right, y(v), t["grid"] if v else t["axis"]))
        body.append(text(left - 8, y(v) + 4, str(v), t["muted"], 11, "end"))
    for i, day in enumerate(BACKFILL_DAYS):
        body.append(text(x(i), bottom + 18, day, t["muted"], 11, "middle"))
    series = [("Bug backfilled", BACKFILL_BUG), ("Fix backfilled", BACKFILL_FIX)]
    # Legend, above the plot: a line key beside text in ink.
    lx = left
    for (name, _), color in zip(series, t["series"], strict=True):
        body.append(line(lx, 70, lx + 18, 70, color, 2))
        body.append(text(lx + 24, 74, name, t["ink2"], 12))
        lx += 130
    for (_name, values), color in zip(series, t["series"], strict=True):
        path = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(values))
        body.append(
            f'<polyline points="{path}" fill="none" stroke="{color}" stroke-width="2" '
            'stroke-linejoin="round" stroke-linecap="round"/>'
        )
        for i, v in enumerate(values):
            body.append(dot(x(i), y(v), color, t["surface"]))
    # The two points the drill judges, labelled; the others carry no number.
    body.append(text(x(0) + 12, y(1) + 16, "1: its own row", t["ink"], 12, weight=600))
    body.append(text(x(0) + 12, y(1) + 31, "= caught", t["ink2"], 12))
    body.append(text(x(2) + 12, y(0) - 10, "0 = repaired", t["ink"], 12, weight=600))
    # Why, in the right margin.
    notes = [
        ("Each check counts every day", 600),
        ("still holding a bad row.", 400),
        ("", 400),
        ("The first buggy day's 1 is", 600),
        ("its own row: the baseline", 400),
        ("was clean. Only the last", 400),
        ("fixed day sees every day", 400),
        ("rewritten: the repair.", 400),
    ]
    for i, (s, weight) in enumerate(notes):
        body.append(
            text(
                510,
                118 + 18 * i,
                s,
                t["ink" if weight == 600 else "ink2"],
                12,
                weight=weight,
            )
        )
    return svg(
        w,
        h,
        t,
        body,
        "Bad rows counted by the revenue contract during the backfill drill",
    )


def drills_chart(t: dict) -> str:
    w, h = 760, 300
    label_w, left, right, top = 236, 252, 470, 86
    band, bar = 44, 22
    xmax = 120_000

    def x(v):
        return left + (right - left) * v / xmax

    body = header(
        t,
        "Failure drills: every committed record lands in bronze exactly once",
        "Kafka records committed during each drill, counted against bronze rows and "
        "distinct offsets (P8 steps 10-12).",
    )
    blue = t["series"][0]
    for i, (name, value) in enumerate(DRILLS):
        cy = top + band * i + band / 2
        body.append(text(label_w, cy + 4, name, t["ink2"], 12, "end"))
        x0, x1 = left, x(value)
        r = 4  # rounded data end, square at the baseline
        body.append(
            f'<path d="M{x0:.1f},{cy - bar / 2:.1f} H{x1 - r:.1f} '
            f"Q{x1:.1f},{cy - bar / 2:.1f} {x1:.1f},{cy - bar / 2 + r:.1f} "
            f"V{cy + bar / 2 - r:.1f} Q{x1:.1f},{cy + bar / 2:.1f} {x1 - r:.1f},{cy + bar / 2:.1f} "
            f'H{x0:.1f} Z" fill="{blue}"/>'
        )
        body.append(text(x1 + 8, cy + 4, f"{value:,}", t["ink"], 12, weight=600))
        body.append(
            text(w - 24, cy + 4, "0 lost, 0 appended twice", t["muted"], 12, "end")
        )
    base_y = top + band * len(DRILLS)
    body.append(line(left, top - 6, left, base_y, t["axis"]))
    body.append(
        text(
            24,
            h - 22,
            "The replay drill's records are duplicates in bronze by design; "
            "silver ignored all of them (same fingerprint before and after).",
            t["muted"],
            11,
        )
    )
    return svg(w, h, t, body, "Records reconciled in each failure drill")


# --- the README's header, headline numbers and architecture ------------------------

# Measured (docs/results.md): label, value, detail.
GLANCE = [
    ("Database commit to Kafka", "< 1 s", "Debezium on the WAL and change streams"),
    ("Commit to queryable bronze", "75 s", "median, max 88 s · target 5 min"),
    ("Commit to gold", "10–36 min", "bounded by the 30-min schedule · target 1 h"),
    ("Commit to the features API", "9.6 s", "median, max 10.6 s · target 1 min"),
    (
        "GDPR erasure, every store",
        "1 h 45 min",
        "sources, lake, Redis, time travel · target 24 h",
    ),
    ("Failure drills", "0 lost", "160,884 records reconciled, 0 appended twice"),
]


def box(x, y, w, h, t, title, sub="", accent=False) -> list[str]:
    """A rounded component box: a title and an optional detail line."""
    out = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="{t["surface"]}" '
        f'stroke="{t["series"][0] if accent else t["border"]}" stroke-width="{1.5 if accent else 1}"/>'
    ]
    cy = y + h / 2 + (-4 if sub else 5)
    out.append(text(x + w / 2, cy, title, t["ink"], 13, "middle", 600))
    if sub:
        out.append(text(x + w / 2, cy + 17, sub, t["ink2"], 11, "middle"))
    return out


def arrow(points, t, label="", dashed=False) -> list[str]:
    """A polyline ending in an arrowhead; an optional label at its middle."""
    d = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    color = t["muted"]
    out = [
        f'<polyline points="{d}" fill="none" stroke="{color}" stroke-width="1.5" '
        f'stroke-linejoin="round"{' stroke-dasharray="4 4"' if dashed else ""} marker-end="url(#head)"/>'
    ]
    if label:
        (x1, y1), (x2, y2) = points[-2], points[-1]
        out.append(
            text((x1 + x2) / 2, (y1 + y2) / 2 - 6, label, t["muted"], 10, "middle")
        )
    return out


def group(x, y, w, h, t, label) -> list[str]:
    return [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{t["panel"]}"/>',
        text(x + 14, y + 22, label.upper(), t["muted"], 10, weight=600),
    ]


def defs(t) -> str:
    return (
        '<defs><marker id="head" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
        f'markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" '
        f'fill="{t["muted"]}"/></marker></defs>'
    )


def banner(t: dict) -> str:
    w, h = 1000, 210
    blue = t["series"][0]
    body = [
        f'<rect x="0" y="0" width="6" height="{h}" fill="{blue}"/>',
        text(44, 74, "theLook CDC Lakehouse", t["ink"], 38, weight=700),
        text(
            44,
            108,
            "Change data capture from PostgreSQL and MongoDB into an Apache Iceberg",
            t["ink2"],
            16,
        ),
        text(
            44,
            130,
            "lakehouse on AWS, served as live features and recommendations.",
            t["ink2"],
            16,
        ),
    ]
    chips = [
        "PostgreSQL · MongoDB",
        "Debezium · Kafka",
        "Spark · Iceberg on S3",
        "Airflow",
        "Redis · Neo4j · FastAPI",
    ]
    x = 44
    for i, chip in enumerate(chips):
        cw = 15 + 7.1 * len(chip)
        body.append(
            f'<rect x="{x:.1f}" y="154" width="{cw:.1f}" height="28" rx="14" '
            f'fill="{blue}" fill-opacity="0.10" stroke="{blue}" stroke-opacity="0.35"/>'
        )
        body.append(text(x + cw / 2, 173, chip, t["ink"], 12, "middle", 500))
        x += cw
        if i < len(chips) - 1:
            body.append(text(x + 11, 173, "→", t["muted"], 13, "middle"))
            x += 22
    return svg(w, h, t, body, "theLook CDC Lakehouse")


def glance(t: dict) -> str:
    w, h = 1000, 250
    cols, gap, tile_h = 3, 14, 110
    tile_w = (w - gap * (cols + 1)) / cols
    body = []
    for i, (label, value, detail) in enumerate(GLANCE):
        x = gap + (tile_w + gap) * (i % cols)
        y = gap + (tile_h + gap) * (i // cols)
        body.append(
            f'<rect x="{x:.1f}" y="{y}" width="{tile_w:.1f}" height="{tile_h}" rx="10" '
            f'fill="{t["panel"]}"/>'
        )
        body.append(text(x + 18, y + 30, label, t["ink2"], 13))
        body.append(text(x + 18, y + 70, value, t["ink"], 30, weight=650))
        body.append(text(x + 18, y + 94, detail, t["muted"], 12))
    return svg(
        w, h, t, body, "Measured: " + "; ".join(f"{a} {b} ({c})" for a, b, c in GLANCE)
    )


def architecture(t: dict) -> str:
    w, h = 1000, 466
    bw, bh = 128, 58
    col = [24, 186, 348, 510, 672, 834]  # box x per column
    body = [defs(t)]
    # Groups first, boxes over them.
    body += group(12, 40, 476, 240, t, "On-prem · Docker Compose")
    body += group(498, 40, 490, 168, t, "AWS · Iceberg on S3, Glue catalog, Athena")
    body += group(498, 222, 490, 230, t, "Serving · on-prem")
    body += group(12, 296, 476, 156, t, "Across the stack")
    # Capture, then the lake: one row.
    top = 110
    mid = top + bh / 2
    body += box(col[0], 76, bw, 52, t, "PostgreSQL 17", "orders, users, items")
    body += box(col[0], 144, bw, 52, t, "MongoDB 8", "events, reviews")
    body += box(col[1], top, bw, bh, t, "Debezium", "change events, Avro")
    body += box(col[1], 200, bw, 46, t, "Schema Registry", "compatibility")
    body += box(col[2], top, bw, bh, t, "Kafka 4", "7 CDC topics", accent=True)
    body += box(col[3], top, bw, bh, t, "bronze", "Spark stream, 60 s")
    body += box(col[4], top, bw, bh, t, "silver", "MERGE latest per key")
    body += box(col[5], top, bw, bh, t, "gold", "star schema, marts")
    turn = col[0] + bw + 18
    body += arrow(
        [(col[0] + bw, 102), (turn, 102), (turn, mid - 8), (col[1], mid - 8)], t
    )
    body += arrow(
        [(col[0] + bw, 170), (turn, 170), (turn, mid + 8), (col[1], mid + 8)], t
    )
    body += arrow([(col[1] + bw / 2, top + bh), (col[1] + bw / 2, 200)], t)
    for a, b in ((1, 2), (2, 3), (3, 4), (4, 5)):
        body += arrow([(col[a] + bw, mid), (col[b], mid)], t)
    # Serving.
    ys = 262
    body += box(col[3], ys, bw, bh, t, "Features stream", "Spark, local mode")
    body += box(col[4], ys, bw, bh, t, "Redis 8", "user features")
    body += box(col[5], ys, bw, bh, t, "Neo4j", "co-purchase graph")
    api_y = 368
    body += box(
        col[4],
        api_y,
        bw * 2 + 34,
        bh,
        t,
        "FastAPI",
        "features · recommendations",
        accent=True,
    )
    kx = col[2] + bw / 2
    body += arrow(
        [(kx, top + bh), (kx, ys + bh / 2), (col[3], ys + bh / 2)], t, "events"
    )
    body += arrow([(col[3] + bw, ys + bh / 2), (col[4], ys + bh / 2)], t)
    gx = col[5] + bw / 2
    body += arrow([(gx, top + bh), (gx, ys)], t)
    body.append(
        text(gx + 8, (top + bh + ys) / 2 + 4, "graph job, daily", t["muted"], 10)
    )
    body += arrow([(col[4] + bw / 2, ys + bh), (col[4] + bw / 2, api_y)], t)
    body += arrow([(gx, ys + bh), (gx, api_y)], t)
    # Orchestration, observability and contracts.
    notes = [
        (
            "Airflow",
            "silver and gold every 30 min, maintenance, graph, erasure, backfill",
        ),
        (
            "Prometheus + Alertmanager",
            "slot, connector and lag alerts, emailed through SNS",
        ),
        (
            "Data contracts",
            "26 ODCS contracts checked in Athena, a daily quality report",
        ),
    ]
    for i, (name, what) in enumerate(notes):
        y = 344 + 36 * i
        body.append(f'<circle cx="31" cy="{y - 4}" r="4" fill="{t["series"][0]}"/>')
        body.append(text(43, y, name, t["ink"], 13, weight=600))
        body.append(text(43, y + 16, what, t["ink2"], 12))
    return svg(
        w,
        h,
        t,
        body,
        "Architecture: PostgreSQL and MongoDB, Debezium, Kafka, bronze, silver and "
        "gold on AWS, Redis, Neo4j and FastAPI",
    )


# The second erasure run (docs/results.md, P8 step 6): step, detail, minutes.
ERASURE_STEPS = [
    ("Request", "user id in the run", None),
    ("Sources", "PostgreSQL,\nMongoDB", 5 / 60),
    ("Wait", "until bronze holds\nthe deletes", 2.5),
    ("Bronze purge", "by the stream,\nits own key", 71),
    ("Redis", "feature keys", None),
    ("Silver and gold", "delete, compact,\nexpire, marts", 29),
    ("Verify", "54 of 54 places\nempty", None),
]


def erasure(t: dict) -> str:
    w, h = 1000, 300
    n, gap = len(ERASURE_STEPS), 14
    bw = (w - 2 * 16 - gap * (n - 1)) / n
    body = [defs(t)]
    for i, (name, detail, _) in enumerate(ERASURE_STEPS):
        x = 16 + i * (bw + gap)
        accent = name == "Bronze purge"
        body.append(
            f'<rect x="{x:.1f}" y="24" width="{bw:.1f}" height="92" rx="10" fill="{t["surface"]}" '
            f'stroke="{t["series"][0] if accent else t["border"]}" stroke-width="{1.5 if accent else 1}"/>'
        )
        body.append(text(x + 12, 50, f"{i + 1}", t["muted"], 11, weight=600))
        body.append(text(x + 12, 70, name, t["ink"], 13, weight=600))
        for j, part in enumerate(detail.split("\n")):
            body.append(text(x + 12, 88 + 15 * j, part, t["ink2"], 11))
        if i < n - 1:
            body += arrow([(x + bw + 1, 70), (x + bw + gap - 1, 70)], t)
    # Where the time goes: one bar, each timed step its share of it.
    total = sum(m for *_, m in ERASURE_STEPS if m)
    x0, x1, y = 16, w - 16, 176
    body.append(text(16, 156, "Where the 1 h 45 min goes", t["ink"], 13, weight=600))
    x = x0
    blue = t["series"][0]
    for name, _, minutes in ERASURE_STEPS:
        if not minutes:
            continue
        seg = (x1 - x0) * minutes / total
        opacity = 1 if name == "Bronze purge" else 0.45
        body.append(
            f'<rect x="{x:.1f}" y="{y}" width="{max(seg - 2, 2):.1f}" height="22" rx="4" '
            f'fill="{blue}" fill-opacity="{opacity}"/>'
        )
        label = f"{minutes:.0f} min" if minutes >= 1 else "5 s"
        if seg > 120:
            body.append(
                text(
                    x + 10,
                    y + 15,
                    f"{name}: {label}",
                    "#ffffff" if opacity == 1 else t["ink"],
                    12,
                    weight=600,
                )
            )
        x += seg
    body.append(
        text(
            16,
            226,
            "Sources 5 s, waiting for bronze 2.5 min: the thin slices at the start.",
            t["muted"],
            12,
        )
    )
    body.append(
        text(
            16,
            246,
            "The purge is copy-on-write: each DELETE rewrites the files that hold the user's rows.",
            t["muted"],
            12,
        )
    )
    body.append(
        text(
            16,
            274,
            "Target (O5): 24 hours. Kafka keeps the change events until its 3-day retention.",
            t["ink2"],
            12,
        )
    )
    return svg(
        w,
        h,
        t,
        body,
        "GDPR erasure: seven steps from the request to verification; 1 h 45 min, 71 min of it the bronze purge",
    )


ROADMAP = [
    ("P1", "CDC from", "PostgreSQL"), ("P2", "MongoDB,", "Terraform, CI"),
    ("P3", "bronze", "on AWS"), ("P4", "silver, gold,", "Airflow"),
    ("P5", "Redis", "features"), ("P6", "Neo4j", "graph"),
    ("P7", "contracts,", "alerting"), ("P8", "erasure, backfill,", "drills"),
    ("P9", "hardening,", "load test"), ("P9b", "Grafana", "dashboards"),
    ("P10", "optional:", "Kinesis, Flink"),
]  # fmt: skip
DONE = 10  # phases built and accepted


def roadmap(t: dict) -> str:
    w, h = 1000, 150
    margin = 64  # room for the end labels
    step = (w - 2 * margin) / (len(ROADMAP) - 1)
    y = 52
    blue = t["series"][0]
    body = [
        f'<line x1="{margin}" y1="{y}" x2="{margin + step * DONE:.1f}" y2="{y}" stroke="{blue}" stroke-width="3"/>',
        f'<line x1="{margin + step * DONE:.1f}" y1="{y}" x2="{w - margin}" y2="{y}" stroke="{t["axis"]}" '
        'stroke-width="3" stroke-dasharray="6 6"/>',
    ]
    for i, (phase, l1, l2) in enumerate(ROADMAP):
        x = margin + step * i
        if i < DONE:  # done: filled, a check mark
            body.append(f'<circle cx="{x:.1f}" cy="{y}" r="15" fill="{blue}"/>')
            body.append(
                f'<path d="M{x - 6:.1f},{y} l4,4 l8,-9" fill="none" stroke="#ffffff" '
                'stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
            )
        else:  # next, then optional: an outlined circle
            ring = blue if i == DONE else t["axis"]
            body.append(
                f'<circle cx="{x:.1f}" cy="{y}" r="15" fill="{t["surface"]}" stroke="{ring}" stroke-width="2.5"/>'
            )
        body.append(
            text(x, 20, phase, t["ink"] if i <= DONE else t["muted"], 13, "middle", 700)
        )
        body.append(
            text(x, y + 38, l1, t["ink2"] if i <= DONE else t["muted"], 11, "middle")
        )
        body.append(
            text(x, y + 53, l2, t["ink2"] if i <= DONE else t["muted"], 11, "middle")
        )
    if DONE < len(ROADMAP) and not ROADMAP[DONE][1].startswith("optional"):
        body.append(
            text(margin + step * DONE, y + 74, "next", t["ink"], 11, "middle", 700)
        )
    done = ", ".join(p for p, _, _ in ROADMAP[:DONE])
    rest = ", ".join(p for p, _, _ in ROADMAP[DONE:])
    return svg(w, h, t, body, f"Roadmap: {done} done; {rest} optional")


# One change, commit to Athena (docs/architecture.md, section 3).
PARTICIPANTS = [
    "generator",
    "PostgreSQL",
    "Debezium",
    "Kafka",
    "bronze stream",
    "Iceberg on S3",
    "transform DAG",
]
# (phase label, [(from, to, label, dashed)]); participants by index.
SEQUENCE = [
    ("Capture · under 1 s", [
        (0, 1, "INSERT order, COMMIT", False),
        (1, 2, "decoded WAL record, from the slot", False),
        (2, 3, "Avro change event", False),
        (2, 1, "confirm the LSN: the slot advances", True),
    ]),
    ("Ingestion · queryable in 75 s (median)", [
        (4, 3, "read new offsets, every 60 s", False),
        (4, 5, "append per table, then the ledger row", False),
    ]),
    ("Refinement · every 30 min, gold in 10–36 min", [
        (6, 5, "silver: MERGE the latest version per key", False),
        (6, 5, "gold: dimensions, facts, marts", False),
    ]),
]  # fmt: skip


def sequence(t: dict) -> str:
    w = 1000
    xs = [
        76 + i * (w - 152) / (len(PARTICIPANTS) - 1) for i in range(len(PARTICIPANTS))
    ]
    row, band_gap, top = 38, 40, 82
    n_rows = sum(len(m) for _, m in SEQUENCE)
    h = top + n_rows * row + len(SEQUENCE) * band_gap + 20
    blue = t["series"][0]
    body = [defs(t)]
    # Phase bands first, under everything.
    y = top
    bands = []
    for label, messages in SEQUENCE:
        height = band_gap + len(messages) * row
        body.append(
            f'<rect x="10" y="{y:.1f}" width="{w - 20}" height="{height - 8:.1f}" rx="10" fill="{t["panel"]}"/>'
        )
        bands.append((y, messages, label))
        y += height
    # Lifelines and participant boxes.
    for x, name in zip(xs, PARTICIPANTS, strict=True):
        body.append(
            f'<line x1="{x:.1f}" y1="56" x2="{x:.1f}" y2="{h - 16}" stroke="{t["border"]}" stroke-width="1.5"/>'
        )
        bw = 7.4 * len(name) + 24
        body.append(
            f'<rect x="{x - bw / 2:.1f}" y="16" width="{bw:.1f}" height="38" rx="8" fill="{t["surface"]}" stroke="{t["border"]}"/>'
        )
        body.append(text(x, 40, name, t["ink"], 13, "middle", 600))
    # Phase labels over the lifelines, with a halo.
    for y0, _, label in bands:
        body.append(
            text(24, y0 + 21, label.upper(), t["muted"], 10, weight=600).replace(
                "<text ",
                f'<text stroke="{t["panel"]}" stroke-width="5" paint-order="stroke" ',
                1,
            )
        )
    # Messages, numbered.
    k = 0
    for y0, messages, _ in bands:
        for i, (a, b, label, dashed) in enumerate(messages):
            k += 1
            yy = y0 + band_gap + i * row + 12
            x1, x2 = xs[a], xs[b]
            gap = 6 if x2 > x1 else -6
            body += arrow([(x1 + gap, yy), (x2 - gap, yy)], t, dashed=dashed)
            # Beside the number, towards the arrow's end; a halo in the band's
            # colour so the lifelines pass behind the words.
            lx, anchor = (x1 + 16, "start") if x2 > x1 else (x1 - 16, "end")
            body.append(
                text(lx, yy - 8, label, t["ink2"], 12, anchor).replace(
                    "<text ",
                    f'<text stroke="{t["panel"]}" stroke-width="5" paint-order="stroke" ',
                    1,
                )
            )
            body.append(f'<circle cx="{x1:.1f}" cy="{yy:.1f}" r="9" fill="{blue}"/>')
            body.append(text(x1, yy + 4, str(k), "#ffffff", 10, "middle", 700))
    return svg(
        w,
        h,
        t,
        body,
        "One change from commit to Athena: capture in under a second, bronze in 75 s, gold within 10 to 36 minutes",
    )


def exactly_once(t: dict) -> str:
    w, h = 1000, 250
    body = [defs(t)]
    blue = t["series"][0]

    def node(x, y, bw, bh, title, sub, accent=False, pill=False):
        r = bh / 2 if pill else 10
        out = [
            f'<rect x="{x}" y="{y}" width="{bw}" height="{bh}" rx="{r}" fill="{t["surface"]}" '
            f'stroke="{blue if accent else t["border"]}" stroke-width="{1.5 if accent else 1}"/>',
            text(
                x + bw / 2,
                y + bh / 2 - (3 if sub else -5),
                title,
                t["ink"],
                13,
                "middle",
                600,
            ),
        ]
        for j, part in enumerate(sub.split("\n") if sub else []):
            out.append(
                text(
                    x + bw / 2, y + bh / 2 + 14 + 14 * j, part, t["ink2"], 11, "middle"
                )
            )
        return out

    body += node(16, 74, 150, 64, "batch N", "offsets from\nthe checkpoint", pill=True)
    body += node(
        196,
        64,
        220,
        84,
        "Already written?",
        "a commit on the table marked\nwith this query and batch N",
        accent=True,
    )
    body += node(456, 30, 190, 58, "skip the table", "logged skipped(replay)")
    body += node(456, 124, 190, 58, "append the rows", "commit marked: query, batch N")
    body += node(686, 74, 150, 64, "ledger row, last", "every table's\nsnapshot id")
    body += node(866, 74, 118, 64, "offsets", "committed", pill=True)
    body += arrow([(166, 106), (196, 106)], t)
    body += arrow([(416, 92), (436, 92), (436, 59), (456, 59)], t)
    body.append(text(426, 52, "yes", t["ink"], 11, "end", 600))
    body += arrow([(416, 120), (436, 120), (436, 153), (456, 153)], t)
    body.append(text(430, 170, "no", t["ink"], 11, "end", 600))
    body += arrow([(646, 59), (666, 59), (666, 98), (686, 98)], t)
    body += arrow([(646, 153), (666, 153), (666, 114), (686, 114)], t)
    body += arrow([(836, 106), (866, 106)], t)
    # A crash anywhere: the restart replays batch N from the checkpoint.
    body += arrow([(925, 138), (925, 220), (91, 220), (91, 140)], t, dashed=True)
    body.append(
        text(
            503,
            214,
            "crash anywhere: the restart runs batch N again, and every table already written is skipped",
            t["muted"],
            11,
            "middle",
        )
    )
    return svg(
        w,
        h,
        t,
        body,
        "Exactly once into bronze: a replayed batch skips the tables already marked with it; the ledger row comes last",
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, draw in (
        ("slot-drill", slot_chart),
        ("backfill-drill", backfill_chart),
        ("drills-reconciled", drills_chart),
        ("banner", banner),
        ("glance", glance),
        ("architecture", architecture),
        ("erasure", erasure),
        ("roadmap", roadmap),
        ("sequence", sequence),
        ("exactly-once", exactly_once),
    ):
        for theme_name, theme in THEMES.items():
            path = OUT / f"{name}-{theme_name}.svg"
            path.write_text(draw(theme) + "\n")
            print(path.relative_to(OUT.parent.parent))


if __name__ == "__main__":
    main()
