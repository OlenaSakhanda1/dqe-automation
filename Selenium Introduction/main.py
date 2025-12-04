
import os
import time
import re
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
)
from selenium.webdriver import ActionChains

# ===================== 0) CONTEXT MANAGER =====================
class SeleniumWebDriverContextManager:
    def __init__(self, headless: bool = False):
        self.driver = None
        self.headless = headless

    def __enter__(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        self.driver = webdriver.Chrome(options=options)
        return self.driver

    def __exit__(self, exc_type, exc_value, traceback):
        if self.driver:
            try:
                self.driver.quit()
            except WebDriverException:
                pass


# ===================== 1) TABLE (SVG → CSV у 3 колонки, як у твоєму підході) =====================
# 1) TABLE: SVG → 3 колонки → (опційно) CSV
def extract_table(driver, save_csv_path=None):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

    try:
        wait = WebDriverWait(driver, 10)
        headers = {"Facility Type", "Visit Date", "Average Time Spent"}

        cells = []
        # XPath (основний)
        try:
            xp = (By.XPATH, "//*[local-name()='text' and contains(@class,'cell-text')]")
            wait.until(EC.visibility_of_element_located(xp))
            cells = driver.find_elements(*xp)
        except TimeoutException:
            pass
        # CSS (fallback)
        if not cells:
            try:
                wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "text.cell-text")))
                cells = driver.find_elements(By.CSS_SELECTOR, "text.cell-text")
            except TimeoutException:
                pass
        # ClassName (fallback)
        if not cells:
            try:
                wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "cell-text")))
                cells = driver.find_elements(By.CLASS_NAME, "cell-text")
            except TimeoutException:
                pass

        values = [c.text.strip() for c in cells if c.text and c.text.strip()]
        clean = [v for v in values if v not in headers]

        rows = len(clean) // 3
        if rows == 0:
            df = pd.DataFrame(columns=["facility_type", "visit_date", "avg_time_spent"])
        else:
            df = pd.DataFrame({
                "facility_type": clean[0:rows],
                "visit_date":    clean[rows:rows*2],
                "avg_time_spent":clean[rows*2:rows*3],
            })

        if save_csv_path:
            os.makedirs(os.path.dirname(save_csv_path), exist_ok=True)
            df.rename(columns={
                "facility_type": "Facility Type",
                "visit_date": "Visit Date",
                "avg_time_spent": "Average Time Spent"
            }).to_csv(save_csv_path, index=False)

        return df

    except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
        return pd.DataFrame(columns=["facility_type", "visit_date", "avg_time_spent"])


# 2) DOUGHNUT: без скриптів; скріншоти + CSV per filter
def extract_doughnut_chart(driver, screenshots_dir, csv_dir, timeout_update_sec=5.0):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from selenium.webdriver import ActionChains

    datasets = []
    try:
        wait = WebDriverWait(driver, 10)
        chart = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.js-plotly-plot")))
        os.makedirs(screenshots_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Initial screenshot
        idx = 0
        shot = os.path.join(screenshots_dir, f"screenshot{idx}.png")
        driver.save_screenshot(shot)

        # Order of labels from legend (.scrollbox -> .traces)
        labels = []
        try:
            box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "scrollbox")))
            for t in box.find_elements(By.CLASS_NAME, "traces"):
                lab = ""
                for xp in (".//span[contains(@class,'legendtext')]", ".//span", ".//*[local-name()='text']"):
                    try:
                        e = t.find_element(By.XPATH, xp)
                        txt = (e.text or "").strip()
                        if txt:
                            lab = txt; break
                    except Exception:
                        pass
                labels.append(lab)
        except TimeoutException:
            labels = []

        # Values from donut slices (g.slice -> g.slicetext text)
        val_map = {}
        for s in driver.find_elements(By.XPATH, "//*[name()='g' and contains(@class,'slice')]"):
            nodes = s.find_elements(By.XPATH, ".//*[name()='g' and contains(@class,'slicetext')]//*[name()='text']")
            raw = " ".join([(n.text or "").strip() for n in nodes if (n.text or "").strip()]).strip()
            if not raw:
                continue
            m = re.findall(r"(\d+(?:[.,]\d+)?)", raw)
            value = m[-1].replace(",", ".") if m else ""
            label = raw[:-len(m[-1])].strip() if m else raw
            if label:
                val_map[label] = value

        pairs = [{"label": lab, "value": val_map.get(lab, "")} for lab in labels]
        datasets.append({"index": idx, "labels": labels, "values": [p["value"] for p in pairs], "screenshot": shot})
        save_doughnut_data([datasets[-1]], csv_dir)

        # Iterate legend filters
        for item in chart.find_elements(By.CSS_SELECTOR, ".legendtoggle"):
            prev_values = datasets[-1]["values"]
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(item))
            try:
                item.click()
            except WebDriverException:
                ActionChains(driver).move_to_element(item).click().perform()

            # Wait for values change (simple polling on slices)
            deadline = time.time() + timeout_update_sec
            while time.time() < deadline:
                # re-read labels (order may change)
                labels = []
                try:
                    box = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "scrollbox")))
                    for t in box.find_elements(By.CLASS_NAME, "traces"):
                        lab = ""
                        for xp in (".//span[contains(@class,'legendtext')]", ".//span", ".//*[local-name()='text']"):
                            try:
                                e = t.find_element(By.XPATH, xp)
                                txt = (e.text or "").strip()
                                if txt: lab = txt; break
                            except Exception:
                                pass
                        labels.append(lab)
                except TimeoutException:
                    pass

                val_map = {}
                for s in driver.find_elements(By.XPATH, "//*[name()='g' and contains(@class,'slice')]"):
                    nodes = s.find_elements(By.XPATH, ".//*[name()='g' and contains(@class,'slicetext')]//*[name()='text']")
                    raw = " ".join([(n.text or "").strip() for n in nodes if (n.text or "").strip()]).strip()
                    if not raw: continue
                    m = re.findall(r"(\d+(?:[.,]\d+)?)", raw)
                    value = m[-1].replace(",", ".") if m else ""
                    label = raw[:-len(m[-1])].strip() if m else raw
                    if label:
                        val_map[label] = value

                cur_values = [val_map.get(lab, "") for lab in labels]
                if cur_values != prev_values:
                    break
                time.sleep(0.2)

            idx += 1
            shot = os.path.join(screenshots_dir, f"screenshot{idx}.png")
            driver.save_screenshot(shot)
            datasets.append({"index": idx, "labels": labels, "values": cur_values, "screenshot": shot})
            save_doughnut_data([datasets[-1]], csv_dir)

        return datasets

    except TimeoutException:
        return []


# 3) SAVE: donut datasets → CSV
def save_doughnut_data(datasets, csv_dir):
    try:
        os.makedirs(csv_dir, exist_ok=True)
        for ds in datasets:
            labels = ds.get("labels", [])
            values = ds.get("values", [])
            out = os.path.join(csv_dir, f"doughnut{ds.get('index', 0)}.csv")
            pd.DataFrame(zip(labels, values),
                         columns=["Facility Type", "Min Average Time Spent"]).to_csv(out, index=False)
    except OSError:
        pass


# ===================== __main__ =====================
if __name__ == "__main__":
    html_file_path = os.path.abspath("report.html")
    screenshots_dir = os.path.abspath("screenshots")
    csv_dir = os.path.abspath("csv")
    table_csv_path = os.path.join(csv_dir, "table.csv")

    with SeleniumWebDriverContextManager(headless=False) as driver:
        driver.get(f"file://{html_file_path}")
        driver.execute_script("document.body.style.zoom='75%'")
        time.sleep(0.5)

        # 2) SVG-таблиця → 3 колонки → CSV (підхід із твоєї функції)
        extract_table(driver, save_csv_path=table_csv_path)

        # 3) Donut: скріншоти + CSV per filter (без JS)
        extract_doughnut_chart(driver, screenshots_dir=screenshots_dir, csv_dir=csv_dir)
