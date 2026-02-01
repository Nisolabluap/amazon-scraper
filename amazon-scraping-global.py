# ---------- DISTUTILS SHIM FOR PYTHON 3.12+ ----------
try:
    import distutils
except ModuleNotFoundError:
    # Redirect to setuptools._distutils
    import setuptools._distutils as distutils
    import sys
    sys.modules['distutils'] = distutils

import csv
import time
import re
import requests
import paramiko
import io
from io import StringIO
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import random
import undetected_chromedriver as uc
import os
import logging
from threading import Thread
import subprocess

def get_chrome_major_version():
    import subprocess, re

    candidates = ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser"]
    for binary in candidates:
        try:
            output = subprocess.check_output([binary, "--version"], stderr=subprocess.DEVNULL).decode()
            match = re.search(r"\d+\.\d+\.\d+\.\d+", output)
            if match:
                return int(match.group().split(".")[0])
        except (FileNotFoundError, subprocess.CalledProcessError):
            continue
    raise RuntimeError("No Chrome/Chromium binary found")

chrome_major = get_chrome_major_version()
print(f"Detected Chrome major version: {chrome_major}")

# ---------- PRINT HELPER ----------
def p(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------- CONFIG ----------
csv_url = "http://4s10829.de.dedi4281.your-server.de/Marketplace-Check/ECDRivals/links.csv"

host = "213.133.105.210"
port = 22
username = "qsqfbm_96"
password = "bH9EqckjfuJ16zKM"
remote_path = "/ECDRivals/links_automated.csv"

STOP_FLAG_FILE = "/root/scraper_stop.flag"

logging.basicConfig(
    filename="scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- LOAD CSV ----------
def load_csv():
    p("Loading CSV")
    response = requests.get(csv_url)
    response.raise_for_status()
    csv_file = StringIO(response.text)
    reader = csv.DictReader(csv_file, delimiter=";")
    rows = []
    header = reader.fieldnames
    for r in reader:
        r["SkippedReason"] = ""
        rows.append(r)
    if "SkippedReason" not in header:
        header.append("SkippedReason")
    if "Timestamp" not in header:
        header.append("Timestamp")
    p(f"CSV loaded: {len(rows)} rows")
    return rows, header

# ---------- SAVE CSV ----------
def save_csv_to_sftp(rows, header):
    p("Saving CSV to SFTP")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=header, delimiter=";")
    writer.writeheader()
    writer.writerows(rows)
    csv_data = output.getvalue()
    output.close()

    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    with sftp.open(remote_path, "w") as f:
        f.write(csv_data)
    sftp.close()
    transport.close()
    p("CSV saved back to SFTP")

# ---------- STOP FLAG ----------
def stop_requested():
    return os.path.exists(STOP_FLAG_FILE)

# ---------- AMAZON BLOCK DETECTION ----------
def amazon_blocked(driver):
    try:
        page_text = driver.page_source.lower()
        allowed_404_messages = [
            "looking for something?",
            "we’re sorry. the web address you entered",
            "suchen sie etwas?",
            "keine funktionsfähige seite"
        ]
        if any(msg in page_text for msg in allowed_404_messages):
            return False
        if driver.find_elements(By.XPATH, "//form[contains(@action,'captcha')]"):
            return True
        blocked_paths = ["/captcha/", "/errors/validatecaptcha", "/ap/signin"]
        if any(p in driver.current_url.lower() for p in blocked_paths):
            return True
        block_phrases = [
            "enter the characters you see below",
            "sorry, we just need to make sure you're not a robot",
            "robot check",
            "geben sie die unten angezeigten zeichen ein",
            "wir müssen sicherstellen, dass sie kein roboter sind"
        ]
        if any(p in page_text for p in block_phrases):
            return True
        if not driver.page_source.strip():
            return True
    except Exception:
        return True
    return False

# ---------- SCRAPER ----------
def start_scrape():
    p("=== Starting Amazon Scraper ===")
    p(f"Loading CSV from {csv_url}")

    blocked = False
    rows_to_update, header = load_csv()
    now = datetime.now()
    scraped_cache = {}

    p("Starting headless Chrome")
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(
        options=options,
        version_main=chrome_major,
        use_subprocess=True
    )
    wait = WebDriverWait(driver, 10)
    p("Chrome started")

    # ---------- AMAZON PREP ----------
    p("Preparing Amazon site")
    try:
        driver.get("https://www.amazon.de")
        time.sleep(1)
        try: driver.find_element(By.XPATH, '//button[text()="Weiter shoppen"]').click()
        except NoSuchElementException: pass
        try: driver.find_element(By.ID, "sp-cc-accept").click()
        except NoSuchElementException: pass

        try:
            wait.until(EC.element_to_be_clickable((By.ID, "glow-ingress-block"))).click()
            postal_input = wait.until(EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput")))
            postal_input.clear()
            postal_input.send_keys("50667")
            driver.find_element(By.XPATH, '//input[@aria-labelledby="GLUXZipUpdate-announce"]').click()
            time.sleep(1)
            try: wait.until(EC.element_to_be_clickable((By.ID, "GLUXConfirmClose"))).click()
            except: pass
        except: pass

        try:
            driver.get(
                "https://www.amazon.de/customer-preferences/edit?"
                "ref_=icp_cop_flyout_change&preferencesReturnUrl=%2F%3Flanguage%3Den_GB%26currency%3DEUR"
            )
            time.sleep(2)
            try:
                german_label = wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'div[data-a-input-name="lop"] label'))
                )
                german_label.click()
            except: pass
            dropdown = wait.until(EC.presence_of_element_located((By.ID, "icp-currency-dropdown")))
            Select(dropdown).select_by_value("EUR")
            save_button = wait.until(EC.presence_of_element_located((By.ID, "icp-save-button")))
            driver.execute_script("arguments[0].click();", save_button)
            time.sleep(2)
        except: pass
    except Exception as e:
        logging.warning(f"Amazon prep failed: {e}")

    total = len(rows_to_update)

    # ---------- MAIN SCRAPE LOOP ----------
    for idx, row in enumerate(rows_to_update, start=1):
        p(f"Processing row {idx}/{total}")

        url = row.get("Link", "").strip()
        p(f"URL: {url}")

        if not url:
            p("Empty URL → skipped")
            row["SkippedReason"] = ""
            continue

        if stop_requested():
            p("STOP FLAG detected")
            break

        if url in scraped_cache:
            p("Using cached result")
            for k, v in scraped_cache[url].items():
                row[k] = v
            continue

        if row.get("Inventory", "").strip().upper() == "NA":
            p("Inventory NA → skipped")
            row["SkippedReason"] = "Inventory NA, skipped."
            scraped_cache[url] = {"SkippedReason": row["SkippedReason"]}
            continue

        ts_str = row.get("Timestamp", "")
        if ts_str:
            try:
                ts = datetime.strptime(ts_str, "%d.%m.%Y %H:%M")
                if now - ts < timedelta(hours=24):
                    p("Scraped <24h ago → skipped")
                    row["SkippedReason"] = "Scraped <24h ago."
                    scraped_cache[url] = {"SkippedReason": row["SkippedReason"]}
                    continue
            except: pass

        original_asin_match = re.search(r'/(?:dp|gp/product)/([A-Z0-9]{10})', url)
        if not original_asin_match:
            p("Invalid ASIN")
            row["SkippedReason"] = "Invalid ASIN in link."
            scraped_cache[url] = {"SkippedReason": row["SkippedReason"]}
            continue

        original_asin = original_asin_match.group(1)
        p(f"Original ASIN: {original_asin}")

        try:
            p("Opening product page")
            driver.get(url)
            time.sleep(random.uniform(1, 3))

            if amazon_blocked(driver):
                p("AMAZON BLOCK / CAPTCHA")
                blocked = True
                row["SkippedReason"] = "Blocked by Amazon / CAPTCHA."
                continue

            current_asin_match = re.search(r'/(?:dp|gp/product)/([A-Z0-9]{10})', driver.current_url)
            if current_asin_match:
                current_asin = current_asin_match.group(1)
                if current_asin != original_asin:
                    p(f"ASIN redirected {original_asin} → {current_asin}")
                    row["SkippedReason"] = f"{original_asin} redirected to {current_asin}, original ASIN might be OOS."
                    scraped_cache[url] = {"SkippedReason": row["SkippedReason"]}
                    continue
            else:
                p("No ASIN after redirect")
                row["SkippedReason"] = "No ASIN found after redirect."
                scraped_cache[url] = {"SkippedReason": row["SkippedReason"]}
                continue

            try:
                availability_text = driver.find_element(By.ID, "availability").text.strip().lower()
            except:
                availability_text = ""
            p(f"Availability: {availability_text}")

            if any(x in availability_text for x in [
                "derzeit nicht verfügbar","nicht auf lager",
                "currently unavailable","out of stock"
            ]):
                p("Out of stock")
                row["SkippedReason"] = "Not in stock."
                scraped_cache[url] = {"SkippedReason": row["SkippedReason"]}
                continue

            p("Checking BuyBox")
            has_buybox = False
            for bid in ["add-to-cart-button","buy-now-button"]:
                try:
                    if driver.find_element(By.ID, bid).is_displayed():
                        has_buybox = True
                        break
                except: pass

            if not has_buybox:
                p("No BuyBox")
                row["SkippedReason"] = "No BuyBox / not purchasable."
                scraped_cache[url] = {"SkippedReason": row["SkippedReason"]}
                continue

            p("Extracting data")

            try: row["Title"] = driver.find_element(By.ID, "title").text
            except: row["Title"] = ""

            try:
                price_el = driver.execute_script(
                    'return document.querySelector("#corePriceDisplay_desktop_feature_div span.aok-offscreen")'
                )
                raw_price = driver.execute_script("return arguments[0].textContent.trim()", price_el)
                row["Price"] = raw_price.replace("€","").replace("EUR","").replace("\u00a0","").strip()
            except: row["Price"] = "0"

            try:
                ship_el = driver.execute_script(
                    'return document.querySelector("#mir-layout-DELIVERY_BLOCK span[data-csa-c-delivery-price]")'
                )
                shipping_price = "0"
                if ship_el:
                    raw_ship = driver.execute_script(
                        "return arguments[0].getAttribute('data-csa-c-delivery-price')", ship_el
                    )
                    if raw_ship:
                        shipping_price = raw_ship.replace("€","").replace("EUR","").replace("\u00a0","").strip()
                row["Shipping"] = shipping_price
            except: row["Shipping"] = "0"

            try:
                row["Image"] = driver.find_element(By.ID, "imgTagWrapperId") \
                    .find_element(By.TAG_NAME,"img").get_attribute("src")
            except: row["Image"] = ""

            row["Timestamp"] = datetime.now().strftime("%d.%m.%Y %H:%M")
            scraped_cache[url] = {
                k: row[k] for k in ["Title","Price","Shipping","Image","Timestamp","SkippedReason"]
            }

            p("Row scraped OK")

        except Exception as e:
            p(f"ERROR: {e}")
            row["SkippedReason"] = f"Error: {e}"
            scraped_cache[url] = {"SkippedReason": row["SkippedReason"]}
            continue

        # ---------- PERIODIC SAVE ----------
        if idx % 50 == 0:
            p(f"Checkpoint save at row {idx}")
            try:
                save_csv_to_sftp(rows_to_update, header)
            except Exception as e:
                p(f"Checkpoint save failed: {e}")

    p("Closing Chrome")
    driver.quit()

    save_csv_to_sftp(rows_to_update, header)

    p("Scraping finished")

# ---------- RUN SCRAPER ----------
if __name__ == "__main__":
    start_scrape()





