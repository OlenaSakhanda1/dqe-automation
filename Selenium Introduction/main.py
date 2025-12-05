import csv
import os
import time
from typing import List, Tuple, Dict, Any
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

GRAPH_ID = "f2be9861-78e2-4f00-aa5f-9778ee33830a"

# ---------- Driver & Page ----------
def init_driver(headless: bool = False) -> Tuple[webdriver.Edge, WebDriverWait]:
    try:
        opts = Options()
        # if headless: opts.add_argument("--headless=new")
        driver = webdriver.Edge(options=opts)
        wait = WebDriverWait(driver, 20)
        return driver, wait
    except Exception as e:
        raise RuntimeError(f"Driver initialization failed: {e}")

def open_page(driver: webdriver.Edge, report_path: str) -> None:
    try:
        driver.get(os.path.abspath(report_path))
    except Exception as e:
        raise RuntimeError(f"Failed to open page '{report_path}': {e}")

def locate_graph(wait: WebDriverWait):
    try:
        return wait.until(EC.presence_of_element_located((By.ID, GRAPH_ID)))
    except Exception:
        try:
            return wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.plotly-graph-div.js-plotly-plot")
            ))
        except Exception as e:
            raise RuntimeError(f"Plotly graph container not found: {e}")

def get_svg(graph, wait: WebDriverWait):
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "svg.main-svg")))
        return graph.find_element(By.CSS_SELECTOR, "svg.main-svg")
    except Exception as e:
        raise RuntimeError(f"SVG not found: {e}")

def capture(target, fallback, filename: str) -> None:
    try:
        target.screenshot(filename)
    except Exception:
        try:
            fallback.screenshot(filename)
        except Exception as e:
            raise RuntimeError(f"Screenshot '{filename}' failed: {e}")

# ---------- Plotly helpers ----------
def js_get_pie_state(driver: webdriver.Edge, graph_el) -> Dict[str, Any]:
    try:
        return driver.execute_script("""
          const el = arguments[0];
          if (!el) return {labels:[], values:[], hiddenlabels:[], visiblePairs:[]};
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
        raise RuntimeError(f"Failed to read pie state: {e}")

def js_set_hiddenlabels(driver: webdriver.Edge, graph_el, hidden: List[str]) -> None:
    try:
        driver.execute_script("""
          const el = arguments[0], hidden = arguments[1] || [];
          if (window.Plotly && el) Plotly.relayout(el, {hiddenlabels: hidden});
        """, graph_el, hidden)
    except Exception as e:
        raise RuntimeError(f"Failed to set hiddenlabels: {e}")

def wait_hiddenlabels_change(driver: webdriver.Edge, graph_el, before: List[str], timeout: float = 10.0) -> None:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: js_get_pie_state(d, graph_el).get("hiddenlabels", []) != before
        )
    except Exception:
        time.sleep(0.5)

# ---------- Required task functions ----------
def extract_table(driver: webdriver.Edge, graph) -> Dict[str, Any]:
    """
    Extract Plotly 'table' trace data if present. Returns {'header': [...], 'cells': [...]} or {}.
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
        raise RuntimeError(f"Failed to extract table: {e}")

def extract_doughnut_chart(driver: webdriver.Edge, graph, svg, screenshot_index: int) -> Tuple[List[List], int]:
    """
    Capture a screenshot and return visible (label, value) pairs of the doughnut chart at current state.
    """
    try:
        driver.execute_script(
            "const s=document.querySelector('svg.main-svg'); if(s){s.style.pointerEvents='all';}"
        )
        time.sleep(0.2)
        capture(svg, graph, f"screenshot{screenshot_index}.png")
        state = js_get_pie_state(driver, graph)
        if not state.get("pieExists", False):
            print("⚠️ No doughnut/pie trace present")
        return state.get("visiblePairs", []), screenshot_index + 1
    except Exception as e:
        raise RuntimeError(f"Failed to extract doughnut chart: {e}")

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
        print(f"✅ Saved {filename}")
        return csv_index + 1
    except Exception as e:
        raise RuntimeError(f"Failed to save doughnut CSV: {e}")

# ---------- Orchestration ----------
def run(headless: bool = False, report_path: str = "report.html") -> None:
    driver, wait = init_driver(headless=headless)
    try:
        open_page(driver, report_path)
        graph = locate_graph(wait)
        svg = get_svg(graph, wait)

        # 1) Initial screenshot + CSV for doughnut
        screenshot_idx = 0
        csv_idx = 0
        pairs, screenshot_idx = extract_doughnut_chart(driver, graph, svg, screenshot_idx)
        csv_idx = save_doughnut_data(pairs, csv_idx)

        # 2) Iterate filters: one label visible at a time
        state = js_get_pie_state(driver, graph)
        labels = state.get("labels", [])
        if labels:
            for lbl in labels:
                before = js_get_pie_state(driver, graph).get("hiddenlabels", [])
                hidden = [x for x in labels if x != lbl]
                js_set_hiddenlabels(driver, graph, hidden)
                wait_hiddenlabels_change(driver, graph, before)

                pairs, screenshot_idx = extract_doughnut_chart(driver, graph, svg, screenshot_idx)
                csv_idx = save_doughnut_data(pairs, csv_idx)
        else:
            print("⚠️ No pie labels found; skipping filter iteration")

        # 3) Edge case: all hidden
        if labels:
            before_all = js_get_pie_state(driver, graph).get("hiddenlabels", [])
            js_set_hiddenlabels(driver, graph, labels)
            wait_hiddenlabels_change(driver, graph, before_all)

            pairs, screenshot_idx = extract_doughnut_chart(driver, graph, svg, screenshot_idx)
            csv_idx = save_doughnut_data(pairs, csv_idx)

        # 4) Extract table (optional; saved as table.csv if present)
        table_data = extract_table(driver, graph)
        if table_data.get("cells"):
            try:
                header_values = table_data.get("header", [])
                cells_values = table_data.get("cells", [])
                header_row = []
                for col in header_values:
                    if isinstance(col, list) and len(col) > 0:
                        header_row.append(str(col[0]))
                    else:
                        header_row.append("")
                rows = list(zip(*cells_values)) if all(isinstance(c, list) for c in cells_values) else []

                with open("table.csv", "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    if header_row and any(h.strip() for h in header_row):
                        w.writerow(header_row)
                    for r in rows:
                        w.writerow([str(x) for x in r])
                print("✅ Saved table.csv")
            except Exception as e:
                print(f"⚠️ Failed to save table.csv: {e}")
        else:
            print("ℹ️ No Plotly table trace found; skipping table.csv")

        # Restore to initial state
        try:
            js_set_hiddenlabels(driver, graph, [])
        except Exception:
            pass
    finally:
        driver.quit()


if __name__ == "__main__":
    run(headless=False, report_path="report.html")
