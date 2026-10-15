# /// script
# requires-python = ">=3.12"
# dependencies = ["playwright==1.55.0"]
# ///
"""Screenshots of the running UIs for the README (docs/images/screens/).

Needs Airflow (its UI reads past runs from its database; the scheduler can
stay stopped), Neo4j, Redis and the API up, onprem/.env, and a headless
Chromium: `uv run --with playwright==1.55.0 python -m playwright install
chromium-headless-shell` (on WSL it may also need libnss3, libnspr4 and
libasound2 on LD_LIBRARY_PATH).

Usage: uv run scripts/readme_screenshots.py [name ...]   (default: all)
"""

import sys
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "images" / "screens"
ENV = dict(
    line.split("=", 1)
    for line in (ROOT / "onprem" / ".env").read_text().splitlines()
    if "=" in line and not line.startswith("#")
)
AIRFLOW = "http://localhost:8088"
# Fully successful runs to show (a graph view colours its nodes by one run).
RUNS = {
    "transform": "scheduled__2026-10-15T02:30:00+00:00",
    "gdpr_erasure": "manual__2026-10-13T05:17:08.798503+00:00",
}


def airflow_login(page: Page) -> None:
    page.goto(f"{AIRFLOW}/auth/login", wait_until="networkidle")
    page.fill("input[name=username]", "admin")
    page.fill("input[type=password]", ENV["AIRFLOW_ADMIN_PASSWORD"])
    page.click("button:has-text('Sign in')")
    page.wait_for_url(lambda url: "/auth/login" not in url, timeout=30_000)


def airflow_graph(page: Page, dag: str, panel: bool) -> None:
    """The DAG's graph view, nodes coloured by one successful run; without
    the run's side panel when the DAG is too wide to read beside it."""
    page.goto(f"{AIRFLOW}/dags/{dag}/runs/{RUNS[dag]}", wait_until="networkidle")
    # The second toggle above the grid switches it to the graph.
    page.locator("button[aria-label*='raph'], [title*='raph']").first.click()
    page.wait_for_timeout(2000)
    if not panel:
        page.locator("button[aria-label='Collapse Details Panel']").click()
        page.wait_for_timeout(1000)
    page.locator(".react-flow__controls-fitview").click()  # zoom to the tasks
    page.wait_for_timeout(1500)
    page.screenshot(path=OUT / f"airflow-{dag}.png")


def neo4j_graph(page: Page) -> None:
    """Neo4j Browser: the most bought product and its 5 companions."""
    page.goto("http://localhost:7474/browser/", wait_until="networkidle")
    page.fill("input[name=username]", "neo4j")
    page.fill("input[name=password]", ENV["NEO4J_PASSWORD"])
    page.click("[data-testid=connection-form-submit]")
    page.wait_for_timeout(4000)
    editor = page.locator(
        ".monaco-editor, .cm-content, [data-testid=activeEditor]"
    ).first
    editor.click()
    page.keyboard.type(
        "MATCH (p:Product {id: 27809})-[r:BOUGHT_WITH]-(o:Product) "
        "RETURN p, r, o ORDER BY r.weight DESC LIMIT 5"
    )
    page.locator("[data-testid=run-button]").first.click()
    dismiss = page.locator("button:has-text('Dismiss')")
    if dismiss.count():
        dismiss.first.click()  # a "new feature" tip over the result
    page.wait_for_timeout(6000)  # the force layout settles
    # The result card: the nearest element holding both the drawing (a
    # canvas) and its "Results overview" side panel.
    frame = page.locator("text=Results overview").locator(
        "xpath=ancestor::*[.//canvas][1]"
    )
    frame.first.screenshot(path=OUT / "neo4j-recommendations.png")


def api_docs(page: Page) -> None:
    """The API's OpenAPI page, the recommendations endpoint tried."""
    page.goto("http://localhost:8000/docs", wait_until="networkidle")
    block = page.locator(".opblock", has_text="/products/{product_id}/recommendations")
    block.locator(".opblock-summary").click()
    block.locator("button.try-out__btn").click()
    block.locator("input[placeholder='product_id']").fill("27809")
    block.locator("button.execute").click()
    block.locator(".responses-wrapper .live-responses-table").wait_for()
    page.wait_for_timeout(1000)
    # From the endpoint's header down to the live response, not the
    # generic examples below it.
    top = block.bounding_box()
    end = block.locator(".live-responses-table").bounding_box()
    page.screenshot(
        path=OUT / "api-docs.png",
        full_page=True,
        clip={
            "x": top["x"],
            "y": top["y"],
            "width": top["width"],
            "height": end["y"] + end["height"] - top["y"] + 12,
        },
    )


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    wanted = set(sys.argv[1:])
    with sync_playwright() as p:
        browser = p.chromium.launch()
        # Short viewport for the graphs: fit view centres one row of tasks.
        page = browser.new_page(viewport={"width": 1440, "height": 560})
        if not wanted or wanted & {"transform", "gdpr_erasure"}:
            airflow_login(page)
            for dag in ("transform", "gdpr_erasure"):
                if not wanted or dag in wanted:
                    airflow_graph(page, dag, panel=dag == "transform")
                    print(f"airflow-{dag}.png")
        page.set_viewport_size({"width": 1440, "height": 860})
        if not wanted or "neo4j" in wanted:
            neo4j_graph(page)
            print("neo4j-recommendations.png")
        if not wanted or "api" in wanted:
            api_docs(page)
            print("api-docs.png")
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
