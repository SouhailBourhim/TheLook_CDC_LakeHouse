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

OUT = Path(__file__).resolve().parent.parent / "docs" / "images"

THEMES = {
    "light": {
        "surface": "#fcfcfb",
        "ink": "#0b0b0b",
        "ink2": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
        "series": ["#2a78d6", "#eb6834"],
    },
    "dark": {
        "surface": "#1a1a19",
        "ink": "#f0efec",
        "ink2": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
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


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, draw in (
        ("slot-drill", slot_chart),
        ("backfill-drill", backfill_chart),
        ("drills-reconciled", drills_chart),
    ):
        for theme_name, theme in THEMES.items():
            path = OUT / f"{name}-{theme_name}.svg"
            path.write_text(draw(theme) + "\n")
            print(path.relative_to(OUT.parent.parent))


if __name__ == "__main__":
    main()
