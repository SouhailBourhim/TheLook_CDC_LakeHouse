# /// script
# requires-python = ">=3.12"
# dependencies = ["playwright==1.55.0"]
# ///
"""Screenshots of the running UIs for the README (docs/images/screens/).

Every capture is 1440 x 900 (16:10), then set in the same browser-window
frame, so the README's gallery rows line up. Needs Airflow's UI (it reads
past runs from its database; the scheduler can stay stopped), Prometheus
(7 days of history), Neo4j, Redis and the API up, onprem/.env, and a
headless Chromium: `uv run --with playwright==1.55.0 python -m playwright
install chromium-headless-shell` (on WSL it may also need libnss3, libnspr4
and libasound2 on LD_LIBRARY_PATH).

Grafana (bi profile, P9b) for the two dashboard captures.

Usage: uv run scripts/readme_screenshots.py [name ...]   (default: all)
"""

import sys
import urllib.parse
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "images" / "screens"
RAW = OUT / "raw"  # unframed captures, git-ignored
ENV = dict(
    line.split("=", 1)
    for line in (ROOT / "onprem" / ".env").read_text().splitlines()
    if "=" in line and not line.startswith("#")
)
W, H = 1440, 900
AIRFLOW = "http://localhost:8088"
PROMETHEUS = "http://localhost:9090"
# Fully successful runs to show (a graph view colours its nodes by one run).
RUNS = {
    "transform": "scheduled__2026-10-15T02:30:00+00:00",
    "gdpr_erasure": "manual__2026-10-13T05:17:08.798503+00:00",
}
# The P8 step 11 drill, 2026-10-14 21:52-23:01 UTC (Prometheus keeps 7 days).
DRILL_END, DRILL_RANGE = "2026-10-14 23:30:00", "2h"


def zoom(page: Page, factor: float) -> None:
    """Show more of a page in the same frame (CSS zoom, crisp at 2x)."""
    page.evaluate(f"document.documentElement.style.zoom = '{factor}'")
    page.wait_for_timeout(800)


def airflow_login(page: Page) -> None:
    page.goto(f"{AIRFLOW}/auth/login", wait_until="networkidle")
    page.fill("input[name=username]", "admin")
    page.fill("input[type=password]", ENV["AIRFLOW_ADMIN_PASSWORD"])
    page.click("button:has-text('Sign in')")
    page.wait_for_url(lambda url: "/auth/login" not in url, timeout=30_000)


def airflow_graph(page: Page, dag: str, run: str | None = None, name: str = "") -> None:
    """The DAG's graph, top to bottom, nodes coloured by one successful run,
    beside that run's details (state, dates, duration)."""
    run = run or RUNS[dag]
    page.goto(f"{AIRFLOW}/dags/{dag}/runs/{run}", wait_until="networkidle")
    page.locator("button[aria-label*='raph'], [title*='raph']").first.click()
    page.wait_for_timeout(1500)
    page.locator("button[aria-label='Options']").first.click()
    page.locator("button", has_text="Left to Right").click()
    page.get_by_text("Top to Bottom", exact=True).last.click()
    page.keyboard.press("Escape")
    page.wait_for_timeout(1500)
    page.locator(".react-flow__controls-fitview").click()
    page.wait_for_timeout(1500)
    page.screenshot(path=RAW / f"{name or 'airflow-' + dag}.png")


def prometheus_drill(page: Page) -> None:
    """The slot drill as Prometheus recorded it: retained WAL, and the alert
    pending then firing."""
    panels = {}
    for i, expr in enumerate(
        [
            "pg_replication_slots_pg_wal_lsn_diff / 1024 / 1024",
            'ALERTS{alertname="ReplicationSlotInactive"}',
        ]
    ):
        panels |= {
            f"g{i}.expr": expr,
            f"g{i}.tab": "graph",
            f"g{i}.range_input": DRILL_RANGE,
            f"g{i}.end_input": DRILL_END,
        }
    # The UI polls for ever: wait for the charts, not for network idle.
    page.goto(f"{PROMETHEUS}/query?{urllib.parse.urlencode(panels)}", wait_until="load")
    page.wait_for_selector(".uplot", timeout=30_000)
    zoom(page, 0.52)  # both panels in one frame
    page.wait_for_timeout(2500)
    page.screenshot(path=RAW / "prometheus-slot-drill.png")


def prometheus_rules(page: Page) -> None:
    """The alert rules: what is watched."""
    page.goto(f"{PROMETHEUS}/rules", wait_until="load")
    page.wait_for_timeout(2500)
    zoom(page, 0.72)  # the four groups in one frame
    page.screenshot(path=RAW / "prometheus-rules.png")


def neo4j_graph(page: Page) -> None:
    """Neo4j Browser: the most bought product and its 5 companions."""
    page.goto("http://localhost:7474/browser/", wait_until="networkidle")
    page.fill("input[name=username]", "neo4j")
    page.fill("input[name=password]", ENV["NEO4J_PASSWORD"])
    page.click("[data-testid=connection-form-submit]")
    # Connected once the query editor's Run button is there.
    page.wait_for_selector("[data-testid=run-button]", timeout=30_000)
    page.wait_for_timeout(1500)
    page.locator("button[title='Collapse Sidebar']").click(timeout=10_000)
    remove = page.locator("button[aria-label='Remove frame']")
    while remove.count():  # the welcome cards; each removal shifts the rest
        remove.first.click()
        page.wait_for_timeout(300)
    page.locator(
        ".monaco-editor, .cm-content, [data-testid=activeEditor]"
    ).first.click()
    page.keyboard.type(
        "MATCH (p:Product {id: 27809})-[r:BOUGHT_WITH]-(o:Product) "
        "RETURN p, r, o ORDER BY r.weight DESC LIMIT 5"
    )
    page.locator("[data-testid=run-button]").first.click()
    page.wait_for_timeout(1500)
    dismiss = page.locator("button:has-text('Dismiss')")
    if dismiss.count():
        dismiss.first.click()  # a "new feature" tip over the result
    page.wait_for_timeout(6000)  # the force layout settles
    zoom(page, 1.2)  # the result fills the frame
    page.screenshot(path=RAW / "neo4j-recommendations.png")


def api_docs(page: Page) -> None:
    """The API's OpenAPI page: the recommendations endpoint, tried live."""
    page.goto("http://localhost:8000/docs", wait_until="networkidle")
    block = page.locator(".opblock", has_text="/products/{product_id}/recommendations")
    block.locator(".opblock-summary").click()
    block.locator("button.try-out__btn").click()
    block.locator("input[placeholder='product_id']").fill("27809")
    block.locator("button.execute").click()
    block.locator(".responses-wrapper .live-responses-table").wait_for()
    zoom(page, 0.8)  # from the endpoint down to the live response
    page.evaluate(
        "window.scrollTo(0, document.querySelector('.opblock.is-open').offsetTop * 0.8 - 12)"
    )
    page.wait_for_timeout(800)
    page.screenshot(path=RAW / "api-docs.png")


def grafana(page: Page, uid: str, since: str) -> None:
    """A Grafana dashboard (bi profile, P9b), its first screen in kiosk mode,
    over `since` (a window without a laptop sleep: one batch spanning hours
    would set a chart's scale). Grafana draws a panel once it is in view: scroll through, back up, then
    wait for the queries (Athena takes seconds)."""
    page.goto(
        f"http://localhost:3000/d/{uid}?orgId=1&kiosk&theme=dark&from={since}&to=now",
        wait_until="networkidle",
    )
    for _ in range(6):
        page.mouse.wheel(0, 400)
        page.wait_for_timeout(300)
    page.mouse.wheel(0, -4000)
    page.wait_for_timeout(12000)
    page.screenshot(path=RAW / f"grafana-{uid.removeprefix('thelook-')}.png")


FRAME = """<!doctype html><html><head><meta charset="utf-8"><style>
body {{ margin: 0; background: transparent; font-family: system-ui, sans-serif; }}
.pad {{ padding: 28px 32px 40px; display: inline-block; }}
.win {{ width: {w}px; border-radius: 14px; overflow: hidden; background: #fff;
  border: 1px solid rgba(0, 0, 0, 0.14);
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.08), 0 18px 40px rgba(0, 0, 0, 0.20); }}
.bar {{ height: 44px; background: #ececef; border-bottom: 1px solid #dcdce0;
  display: flex; align-items: center; padding: 0 18px; gap: 9px; }}
.dot {{ width: 13px; height: 13px; border-radius: 50%; }}
.url {{ margin: 0 auto; transform: translateX(-30px); background: #fff;
  border-radius: 8px; padding: 6px 18px; font-size: 15px; color: #52514e;
  min-width: 420px; text-align: center; }}
img {{ display: block; width: {w}px; }}
</style></head><body><div class="pad"><div class="win">
<div class="bar"><span class="dot" style="background:#ff5f57"></span>
<span class="dot" style="background:#febc2e"></span>
<span class="dot" style="background:#28c840"></span>
<span class="url">{url}</span></div>
<img src="file://{img}"></div></div></body></html>"""


def frame(page: Page, name: str, url: str) -> None:
    """Set a raw capture in a browser window, on a transparent background
    (the frame's shadow then sits on GitHub's light and dark pages alike)."""
    html = RAW / f"{name}.html"
    html.write_text(FRAME.format(w=W, url=url, img=RAW / f"{name}.png"))
    page.set_viewport_size({"width": W + 100, "height": H + 160})
    page.goto(f"file://{html}")
    page.wait_for_timeout(300)
    page.locator(".pad").screenshot(path=OUT / f"{name}.png", omit_background=True)
    html.unlink()


SHOTS = {
    "airflow-transform": (
        "localhost:8088 · dags / transform",
        lambda p: airflow_graph(p, "transform"),
    ),
    "airflow-gdpr_erasure": (
        "localhost:8088 · dags / gdpr_erasure",
        lambda p: airflow_graph(p, "gdpr_erasure"),
    ),
    "prometheus-slot-drill": ("localhost:9090 · query", prometheus_drill),
    "prometheus-rules": ("localhost:9090 · rules", prometheus_rules),
    "neo4j-recommendations": ("localhost:7474 · browser", neo4j_graph),
    "api-docs": ("localhost:8000 · docs", api_docs),
    # The demo walkthrough (docs/demo.md): the transform run that carried
    # drills/demo_order.py's returned order to gold, 2026-10-17.
    "demo-transform-run": (
        "localhost:8088 · dags / transform / the demo order's run",
        lambda p: airflow_graph(
            p,
            "transform",
            "manual__2026-10-17T18:41:21.585024+00:00",
            "demo-transform-run",
        ),
    ),
    "grafana-pipeline": (
        "localhost:3000 · theLook: pipeline health",
        lambda p: grafana(p, "thelook-pipeline", "now-2h"),
    ),
    "grafana-business": (
        "localhost:3000 · theLook: business (gold marts)",
        lambda p: grafana(p, "thelook-business", "now-30d"),
    ),
}


def main() -> int:
    RAW.mkdir(parents=True, exist_ok=True)
    wanted = sys.argv[1:] or list(SHOTS)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        # An explicit locale: Prometheus's UI fails on this host's default
        # ("en-US@posix" is not a valid language tag); UTC like the logs.
        # 2x pixels: the gallery shows each shot at half width.
        page = browser.new_page(
            viewport={"width": W, "height": H},
            device_scale_factor=2,
            locale="en-US",
            timezone_id="UTC",
        )
        if any(name.startswith(("airflow", "demo-")) for name in wanted):
            airflow_login(page)
        for name in wanted:
            url, capture = SHOTS[name]
            page.set_viewport_size({"width": W, "height": H})
            capture(page)
            frame(page, name, url)
            print(f"{name}.png")
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
