"""
Test script to extract detailed data from ONE KPME establishment
to verify the extraction logic works correctly
"""

import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

KPME_URL = "https://kpme.karnataka.gov.in/AllapplicationList.aspx"

def extract_detail_data():
    """Extract full detailed data from first establishment's View popup"""

    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')

    driver = webdriver.Chrome(options=chrome_options)

    try:
        print("Loading KPME portal...")
        driver.get(KPME_URL)
        time.sleep(5)

        # Wait for table
        wait = WebDriverWait(driver, 20)
        table = wait.until(EC.presence_of_element_located((
            By.ID, "ContentPlaceHolder1_gvw_list"
        )))

        rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header
        print(f"Found {len(rows)} establishments\n")

        # Get first row basic data
        first_row = rows[0]
        cells = first_row.find_elements(By.TAG_NAME, "td")

        basic_data = {
            'system_of_medicine': cells[0].text.strip(),
            'category': cells[1].text.strip(),
            'establishment_name': cells[2].text.strip(),
            'address': cells[3].text.strip(),
            'certificate_validity': cells[4].text.strip(),
            'certificate_number': cells[5].text.strip(),
        }

        print("=" * 80)
        print("BASIC TABLE DATA:")
        print("=" * 80)
        for key, value in basic_data.items():
            print(f"{key}: {value}")

        # Click the "View" button for Establishment Details (second View button)
        view_buttons = first_row.find_elements(By.LINK_TEXT, "View")
        print(f"\nFound {len(view_buttons)} View buttons in row")

        if len(view_buttons) >= 2:
            detail_button = view_buttons[1]  # Second View is for Establishment Details

            # Scroll and click
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_button)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", detail_button)
            print("Clicked Establishment Details View button")

            # Wait for popup to load
            time.sleep(3)

            # Try to find the popup window
            print("\n" + "=" * 80)
            print("EXTRACTING DETAILED DATA FROM POPUP:")
            print("=" * 80)

            # Save page source to see the structure
            with open("/tmp/kpme_popup.html", "w") as f:
                f.write(driver.page_source)
            print("Saved page source to /tmp/kpme_popup.html")

            # Try to extract all text from body
            body = driver.find_element(By.TAG_NAME, "body")
            all_text = body.text

            print("\nFull popup text (first 2000 chars):")
            print("-" * 80)
            print(all_text[:2000])
            print("-" * 80)

            # Try to find specific sections by looking for headers
            detailed_data = {}

            # Look for labeled data (key: value pairs)
            try:
                # Find all span elements which often contain labels and values
                spans = driver.find_elements(By.TAG_NAME, "span")
                print(f"\nFound {len(spans)} span elements")

                for span in spans[:30]:  # Check first 30 spans
                    text = span.text.strip()
                    if text and ':' in text:
                        print(f"  - {text[:100]}")
            except Exception as e:
                print(f"Error finding spans: {e}")

            # Try to find tables in the popup
            try:
                tables = driver.find_elements(By.TAG_NAME, "table")
                print(f"\nFound {len(tables)} tables in popup")

                for idx, table in enumerate(tables[:5]):  # Show first 5 tables
                    print(f"\nTable {idx + 1}:")
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    print(f"  Rows: {len(rows)}")

                    # Show first few rows
                    for ridx, row in enumerate(rows[:3]):
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if not cells:
                            cells = row.find_elements(By.TAG_NAME, "th")

                        if cells:
                            cell_texts = [c.text.strip() for c in cells]
                            print(f"    Row {ridx + 1}: {cell_texts}")

            except Exception as e:
                print(f"Error finding tables: {e}")

            # Check if there's an iframe or modal
            try:
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                if iframes:
                    print(f"\nFound {len(iframes)} iframes - data might be in iframe!")
                    # Switch to first iframe
                    driver.switch_to.frame(iframes[0])
                    iframe_text = driver.find_element(By.TAG_NAME, "body").text
                    print(f"Iframe content (first 1000 chars):\n{iframe_text[:1000]}")
                    driver.switch_to.default_content()
            except Exception as e:
                print(f"No iframes found: {e}")

            # Try to find modal by class
            try:
                modal = driver.find_element(By.XPATH, "//div[contains(@class, 'modal-body') or contains(@class, 'modal-content')]")
                print(f"\nFound modal element!")
                modal_text = modal.text
                print(f"Modal text (first 1500 chars):\n{modal_text[:1500]}")
            except:
                print("\nNo modal-body/modal-content found")

            # Try to find any div with id containing 'modal' or 'popup'
            try:
                modal_divs = driver.find_elements(By.XPATH, "//div[contains(@id, 'modal') or contains(@id, 'Modal') or contains(@id, 'popup') or contains(@id, 'Popup')]")
                if modal_divs:
                    print(f"\nFound {len(modal_divs)} divs with modal/popup in ID:")
                    for div in modal_divs[:3]:
                        div_id = div.get_attribute('id')
                        print(f"  - ID: {div_id}")
                        print(f"    Text (first 500 chars): {div.text[:500]}")
            except Exception as e:
                print(f"Error finding modal divs: {e}")

    finally:
        time.sleep(2)  # Keep window open briefly to see
        driver.quit()
        print("\n" + "=" * 80)
        print("Test completed! Check /tmp/kpme_popup.html for full page source")
        print("=" * 80)

if __name__ == "__main__":
    extract_detail_data()
