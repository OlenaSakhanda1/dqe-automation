import os, time, re, json
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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import JavascriptException

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

def extract_values_from_script(driver) -> list | None:
    """
    Searches for a <script> tag containing "values": [[...],[...],[...]] and returns this 3-dimensional array.
    Returns None if not found or unable to parse.
    """
    scripts = driver.find_elements(By.TAG_NAME, "script")  # Locator #3: TAG_NAME
    for script in scripts:
        text = script.get_attribute("innerHTML") or ""
        if '"values"' in text:
            # Extract JSON-like array using two strategies:
            # 1) strict: "values": [[...],[...],[...]]
            m = re.search(r'"values"\s*:\s*(\[\s*\[.*?\]\s*\])', text, re.DOTALL)
            if m:
                payload = m.group(1)
                # Attempt to render to valid JSON
                try:
                    return json.loads(payload)
                except json.JSONDecodeError:
                    # fallback: sometimes arrays can contain single quotes or trailing commas — we're trying to fix it
                    cleaned = (
                        payload
                        .replace("'", '"')
                        .replace(",]", "]")
                        .replace(",}", "}")
                    )
                    try:
                        return json.loads(cleaned)
                    except json.JSONDecodeError:
                        pass
    return None

def validate_values_shape(values: list) -> bool:
    """
    Checks that the array has exactly three subarrays: [Facility[], Date[], Time[]]
    and that their lengths are consistent.
    """
    if not isinstance(values, list) or len(values) != 3:
        return False
    a, b, c = values
    if not (isinstance(a, list) and isinstance(b, list) and isinstance(c, list)):
        return False
    return len(a) == len(b) == len(c) and len(a) > 0

def save_as_csv(values: list, out_path: str) -> None:
    """Saves an array of values ​​in CSV with the columns: Facility Type, Visit Date, Average Time Spent."""
    facility_types, visit_dates, avg_times = values
    rows = list(zip(facility_types, visit_dates, avg_times))
    df = pd.DataFrame(rows, columns=["Facility Type", "Visit Date", "Average Time Spent"])
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)

def extract_doughnut_data(driver):
    js_script = """
    var slices = document.querySelectorAll('.js-plotly-plot g.slice');
    var labels = [];
    var values = [];
    slices.forEach(function(slice) {
        var textEl = slice.querySelector('g.slicetext text');
        if (textEl) {
            var rawText = textEl.textContent.trim();
            var match = rawText.match(/^([A-Za-z\\s]+)([0-9.,]+)$/);
            if (match) {
                var label = match[1].trim();
                var value = parseFloat(match[2].replace(',', '.'));
                labels.push(label);
                values.push(isNaN(value) ? null : value);
            } else {
                labels.push(rawText);
                values.push(null);
            }
        }
    });
    return { labels: labels, values: values };
    """
    result = driver.execute_script(js_script)
    return (result['labels'], result['values']) if result else None

def save_doughnut_csv(labels: List[str], values: List[float], out_path: str):
    """Save doughnut chart data to CSV."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pd.DataFrame(zip(labels, values), columns=["Facility Type", "Min Average Time Spent"]).to_csv(out_path, index=False)

def take_screenshot(driver, out_path: str):
    """Save full-page screenshot."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    driver.save_screenshot(out_path)

def click_legend_item(driver, element) -> None:
    WebDriverWait(driver, 10).until(lambda d: element.is_displayed())

    try:
        driver.execute_script("""
            const el = arguments[0];
            const rect = el.getBoundingClientRect();
            const cx = Math.floor(rect.left + rect.width / 2);
            const cy = Math.floor(rect.top + rect.height / 2);
            const opts = {
                bubbles: true,
                cancelable: true,
                view: window,
                clientX: cx,
                clientY: cy
            };

            el.dispatchEvent(new MouseEvent('mouseover', opts));
            el.dispatchEvent(new PointerEvent('pointerover', opts));
            el.dispatchEvent(new MouseEvent('mousedown', opts));
            el.dispatchEvent(new PointerEvent('pointerdown', opts));
            el.dispatchEvent(new MouseEvent('mouseup', opts));
            el.dispatchEvent(new PointerEvent('pointerup', opts));
            el.dispatchEvent(new MouseEvent('click', opts));
        """, element)

    except JavascriptException:
        ActionChains(driver).move_to_element(element).click().perform()

def get_legend_items(driver) -> List:
    """Return list of legend toggle elements."""
    return driver.find_elements(By.CSS_SELECTOR, ".legendtoggle")

def wait_for_chart_update(driver, prev_values: Optional[List[float]], timeout_sec: float = 3.0) -> Optional[Tuple[List[str], List[float]]]:
    """Wait for chart data to change or timeout."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        snapshot = extract_doughnut_data(driver)
        if snapshot and (prev_values is None or snapshot[1] != prev_values):
            return snapshot
        time.sleep(0.2)
    return snapshot

if __name__ == "__main__":
    file_path = os.path.abspath("report.html")
    csv_out = os.path.abspath(os.path.join("csv", "table.csv"))
    screenshots_dir = os.path.abspath("screenshots")
    csv_doughnut_dir = os.path.abspath("csv")

    try:
        with SeleniumWebDriverContextManager(headless=False) as driver:
            driver.get(f"file:///{file_path}")

            wait = WebDriverWait(driver, 10)

            # Locator #1: CSS Selector — graphic/report container (visible DOM block)
            try:
                container = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.gl-container"))
                )
                print("✅ Container found by CSS Selector: div.gl-container")
            except TimeoutException:
                alt_css = ["div.plotly", "div.js-plotly-plot", "div.main-svg", "div#main"]
                container = None
                for css in alt_css:
                    try:
                        container = wait.until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, css))
                        )
                        print(f"✅ Container found by CSS Selector: {css}")
                        break
                    except TimeoutException:
                        continue
                if container is None:
                    raise NoSuchElementException(
                        "Could not find report container by CSS selectors."
                    )

            # Locator #2: XPath — section title "Last week loaded data" (or similar text)
            try:
                header = wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, "//*[contains(text(),'Last week loaded data')]")
                    )
                )
                print(f"✅ Title found by XPath: {header.text.strip()}")
            except TimeoutException:
                alt_xpaths = [
                    "//*[contains(text(),'Last week') and contains(text(),'data')]",
                    "//*[contains(text(),'Facility Type') and contains(text(),'Average Time Spent')]",
                    "//text[contains(.,'Last week')]",
                ]
                header = None
                for xp in alt_xpaths:
                    try:
                        header = wait.until(EC.presence_of_element_located((By.XPATH, xp)))
                        print(f"✅ Title found by XPath: {xp}")
                        break
                    except TimeoutException:
                        continue

            # Locator #3: TAG_NAME — there must be at least one <script> from the page
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "script")))
                print("✅ <script> found for TAG_NAME.")
            except TimeoutException:
                raise NoSuchElementException("The page is missing <script> tags - data cannot be extracted.")

            # Extracting data from scripts
            values = extract_values_from_script(driver)
            if not values:
                raise NoSuchElementException("Data ('\"values\"') not found in any <script> tag.")

            # Data structure validation
            if not validate_values_shape(values):
                raise ValueError("The 'values' structure is invalid: three levels of subarrays of equal length are expected.")

            # Save CSV
            save_as_csv(values, csv_out)
            print(f"✅ The data is stored in {csv_out}")

            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.js-plotly-plot")))
                print("✅ Doughnut chart container found.")

                screenshot_index = 0
                take_screenshot(driver, os.path.join(screenshots_dir, f"screenshot{screenshot_index}.png"))
                initial_data = extract_doughnut_data(driver)

                if initial_data:
                    save_doughnut_csv(initial_data[0], initial_data[1], os.path.join(csv_doughnut_dir, f"doughnut{screenshot_index}.csv"))
                    print("✅ Initial doughnut chart data saved.")

                legend_items = get_legend_items(driver)
                print(f"ℹ️ Legend items found: {len(legend_items)}")

                for idx, item in enumerate(legend_items, start=1):
                    try:
                        before_values = initial_data[1] if initial_data else None

                        WebDriverWait(driver, 10).until(lambda d: item.is_displayed())
                        click_legend_item(driver, item)

                        time.sleep(10)

                        screenshot_index += 1
                        take_screenshot(driver, os.path.join(screenshots_dir, f"screenshot{screenshot_index}.png"))

                        updated_data = extract_doughnut_data(driver)
                        if updated_data:
                            save_doughnut_csv(updated_data[0], updated_data[1],
                                              os.path.join(csv_doughnut_dir, f"doughnut{screenshot_index}.csv"))
                            print(f"✅ Doughnut data saved for filter #{idx}.")
                        else:
                            save_doughnut_csv([], [],
                                              os.path.join(csv_doughnut_dir, f"doughnut{screenshot_index}.csv"))
                            print(f"⚠️ No doughnut data found for filter #{idx}.")

                    except Exception as e:
                        print(f"❌ Error applying filter #{idx}: {repr(e)}")

            except TimeoutException:
                print("❌ Doughnut chart container not found.")

    except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
        print(f"❌ Error searching/interacting with DOM elements: {e}")
    except WebDriverException as e:
        print(f"❌ WebDriver error: {e}")
    except ValueError as e:
        print(f"❌ Incorrect data: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
