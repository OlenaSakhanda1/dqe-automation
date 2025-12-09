import csv, os, time
from typing import List, Tuple, Dict, Any

from selenium import webdriver
from selenium.webdriver.common.by import By
# from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options

import base64

GRAPH_ID = "f2be9861-78e2-4f00-aa5f-9778ee33830a"

def screenshot_plotly_to_image(driver, graph, out_path):
    data_uri = driver.execute_script("""
        const el = arguments[0];
        return Plotly.toImage(el, {format:'png', width:1200, height:700});
    """, graph)
    b64 = data_uri.split(",", 1)[1]
    with open(out_path, "wb") as f:
        f.write(base64.b64decode(b64))

# ---------- Driver & Page ----------

def init_driver(headless: bool = False) -> Tuple[webdriver.Chrome, WebDriverWait]:
    try:
        opts = Options()
        # driver = webdriver.Edge(options=opts)
        driver = Chrome(options=opts)

        driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
            "width": 1600,  # достатній розмір
            "height": 1200,
            "deviceScaleFactor": 1,  # без масштабування
            "mobile": False,
        })
        wait = WebDriverWait(driver, 20)
        return driver, wait
    except Exception as e:
        print(f"❌ Driver initialization failed: {e}")
        raise

def open_page(driver: webdriver.Chrome, report_path: str) -> None:
    try:
        driver.get(os.path.abspath(report_path))
    except Exception as e:
        print(f"❌ Failed to open page '{report_path}': {e}")
        raise


def locate_graph(wait: WebDriverWait):
    try:
        return wait.until(EC.presence_of_element_located((By.ID, GRAPH_ID)))
    except Exception:
        try:
            return wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.plotly-graph-div.js-plotly-plot")
            ))
        except Exception as e:
            print(f"❌ Plotly graph container not found: {e}")
            raise


def get_svg(graph, wait: WebDriverWait):
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "svg.main-svg")))
        return graph.find_element(By.CSS_SELECTOR, "svg.main-svg")
    except Exception as e:
        print(f"❌ SVG not found: {e}")
        raise


def capture(target, fallback, filename: str) -> None:
    try:
        target.screenshot(filename)
    except Exception as e1:
        print(f"⚠️ Primary screenshot failed ({filename}): {e1}; trying fallback…")
        try:
            fallback.screenshot(filename)
        except Exception as e2:
            print(f"❌ Fallback screenshot failed ({filename}): {e2}")
            raise


# ---------- Plotly helpers ----------

def js_get_pie_state(driver: webdriver.Chrome, graph_el) -> Dict[str, Any]:
    try:
        return driver.execute_script("""
          const el = arguments[0];
          if (!el) return {labels:[], values:[], hiddenlabels:[], visiblePairs:[], pieExists:false};
          const fd = el._fullData || el.data || [];
          const pie = fd.find(t => (t.type || (t._module && t._module.name)) === 'pie');
          const labels = (pie && pie.labels) || [];
          const values = (pie && pie.values) || [];
          const hl = (el && el._fullLayout && el._fullLayout.hiddenlabels) || [];
          const visiblePairs = [];
          for (let i = 0; i < labels.length; i++) {
            if (!hl.includes(labels[i])) visiblePairs.push([labels[i], values[i]]);
          }
          return { labels, values, hiddenlabels: hl, visiblePairs, pieExists: !!pie };
        """, graph_el)
    except Exception as e:
        print(f"❌ Failed to read pie state: {e}")
        raise


def js_set_hiddenlabels(driver: webdriver.Chrome, graph_el, hidden: List[str]) -> None:
    try:
        driver.execute_script("""
          const el = arguments[0], hidden = arguments[1] || [];
          if (window.Plotly && el) { Plotly.relayout(el, {hiddenlabels: hidden}); }
          else { throw new Error('Plotly or element not available'); }
        """, graph_el, hidden)
    except Exception as e:
        print(f"❌ Failed to set hiddenlabels: {e}")
        raise


def wait_hiddenlabels_change(driver: webdriver.Chrome, graph_el, before: List[str], timeout: float = 10.0) -> None:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: js_get_pie_state(d, graph_el).get("hiddenlabels", []) != before
        )
    except Exception as e:
        print(f"⚠️ hiddenlabels change wait timed out or failed: {e}; sleeping fallback…")
        time.sleep(0.5)


# ---------- Required task functions ----------

def extract_table(driver: webdriver.Chrome, graph) -> Dict[str, Any]:
    """
    Extract Plotly 'table' trace data if present.
    Returns {'header': [...], 'cells': [...]} or {}.
    """
    try:
        data = driver.execute_script("""
          const el = arguments[0];
          const fd = el && (el._fullData || el.data) || [];
          const tableTrace = fd.find(t => (t.type || (t._module && t._module.name)) === 'table');
          if (tableTrace && tableTrace.header && tableTrace.cells) {
            return { header: tableTrace.header.values || [], cells: tableTrace.cells.values || [] };
          }
          return {};
        """, graph)
        return data or {}
    except Exception as e:
        print(f"❌ Failed to extract table trace: {e}")
        return {}

def extract_doughnut_chart(driver, graph, svg, screenshot_index: int):
    try:
        driver.execute_script("""
            const el = arguments[0];
            if (window.Plotly && el) {
                Plotly.relayout(el, {autosize:false, width:1200, height:700});
            }
        """, graph)
        time.sleep(0.2)

        out_name = f"screenshot{screenshot_index}.png"
        screenshot_plotly_to_image(driver, graph, out_name)

        print(f"📸 Saved {out_name}")

        state = js_get_pie_state(driver, graph)
        if not state.get("pieExists", False):
            print("⚠️ No doughnut/pie trace present")
        return state.get("visiblePairs", []), screenshot_index + 1
    except Exception as e:
        print(f"❌ Failed to extract doughnut chart: {e}")
        return [], screenshot_index

def save_doughnut_data(pairs: List[List], csv_index: int) -> int:
    """
    Save visible doughnut data to a CSV named sequentially as doughnut{csv_index}.csv
    """
    try:
        filename = f"doughnut{csv_index}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Facility Type", "Min Average Time Spent"])
            for lbl, val in pairs:
                w.writerow([str(lbl), str(val)])
        print(f"💾 Saved {filename}")
        return csv_index + 1
    except Exception as e:
        print(f"❌ Failed to save doughnut CSV: {e}")
        return csv_index


# ---------- Orchestration ----------

def run(headless: bool = False, report_path: str = "report.html") -> None:
    driver, wait = init_driver(headless=headless)
    try:
        open_page(driver, report_path)
        graph = locate_graph(wait)
        svg = get_svg(graph, wait)

        # Initial doughnut

        # Initial doughnut
        screenshot_idx = 0
        csv_idx = 0
        pairs, screenshot_idx = extract_doughnut_chart(driver, graph, svg, screenshot_idx)
        csv_idx = save_doughnut_data(pairs, csv_idx)

        # Iterate: each next state hides one more label than previous
        state = js_get_pie_state(driver, graph)
        labels = state.get("labels", [])
        if labels:
            for hide_count in range(1, len(labels) + 1):
                try:
                    before = js_get_pie_state(driver, graph).get("hiddenlabels", [])
                    hidden = labels[:hide_count]
                    js_set_hiddenlabels(driver, graph, hidden)
                    wait_hiddenlabels_change(driver, graph, before)
                    time.sleep(0.2)
                    pairs, screenshot_idx = extract_doughnut_chart(driver, graph, svg, screenshot_idx)
                    csv_idx = save_doughnut_data(pairs, csv_idx)
                except Exception as e:
                    print(f"⚠️ Progressive hide step {hide_count} failed: {e}")
                    break
        else:
            print("⚠️ No pie labels found; skipping progressive iteration")

        # Extract table (optional save)
        table_data = extract_table(driver, graph)
        if table_data.get("cells"):
            try:
                header_values = table_data.get("header", [])
                cells_values = table_data.get("cells", [])
                header_row = [str(col[0]) if isinstance(col, list) and col else "" for col in header_values]
                rows = list(zip(*cells_values)) if all(isinstance(c, list) for c in cells_values) else []
                with open("table.csv", "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    if header_row and any(h.strip() for h in header_row):
                        w.writerow(header_row)
                    for r in rows:
                        w.writerow([str(x) for x in r])
                print("💾 Saved table.csv")
            except Exception as e:
                print(f"⚠️ Failed to save table.csv: {e}")
        else:
            print("ℹ️ No Plotly table trace found; skipping table.csv")

        # Restore to initial state
        try:
            js_set_hiddenlabels(driver, graph, [])
            print("↩️ Restored initial hiddenlabels state")
        except Exception as e:
            print(f"⚠️ Failed to restore initial state: {e}")

    finally:
        try:
            driver.quit()
            print("✅ Driver closed")
        except Exception as e:
            print(f"⚠️ Driver close failed: {e}")


# --- Precise screenshot of an element even if partly off-screen ---
def _make_visible_and_unclip(driver, el):
    # 1) Прокрутити в центр viewport
    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center', inline:'center'});", el
    )
    time.sleep(0.1)
    # 2) Тимчасово прибрати overflow:hidden у предків (щоб не різало)
    driver.execute_script("""
    const el = arguments[0];
    let node = el;
    while (node && node instanceof Element) {
        const cs = getComputedStyle(node);
        if (cs.overflow === 'hidden' || cs.overflowX === 'hidden' || cs.overflowY === 'hidden') {
            node.__oldOverflow = [node.style.overflow, node.style.overflowX, node.style.overflowY];
            node.style.overflow = 'visible';
            node.style.overflowX = 'visible';
            node.style.overflowY = 'visible';
        }
        node = node.parentElement;
    }
    """, el)


if __name__ == "__main__":
    run(headless=False, report_path="report.html")
