"""
KPME Debug Scraper - Check page structure and take screenshot
"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Setup Chrome
chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920,1080')

driver = webdriver.Chrome(options=chrome_options)

try:
    print("Loading KPME page...")
    driver.get("https://kpme.karnataka.gov.in/AllapplicationList.aspx")
    time.sleep(5)

    # Take screenshot
    screenshot_path = "/tmp/kpme_page.png"
    driver.save_screenshot(screenshot_path)
    print(f"Screenshot saved to {screenshot_path}")

    # Check for tables
    tables = driver.find_elements(By.TAG_NAME, "table")
    print(f"\nFound {len(tables)} tables")

    # Check for table with ID containing GridView
    gridview_tables = driver.find_elements(By.XPATH, "//table[contains(@id, 'GridView') or contains(@id, 'grd')]")
    print(f"Found {len(gridview_tables)} GridView tables")

    # Get all element IDs
    all_ids = driver.execute_script("""
        var elements = document.querySelectorAll('[id]');
        var ids = [];
        for(var i=0; i<Math.min(elements.length, 50); i++) {
            ids.push(elements[i].id);
        }
        return ids;
    """)

    print(f"\nFirst 50 element IDs on page:")
    for id in all_ids[:50]:
        if id:
            print(f"  - {id}")

    # Save page source
    with open("/tmp/kpme_page_source.html", "w") as f:
        f.write(driver.page_source)
    print("\nPage source saved to /tmp/kpme_page_source.html")

    # Try to find any visible rows
    rows = driver.find_elements(By.TAG_NAME, "tr")
    print(f"\nFound {len(rows)} total table rows")

    if tables:
        print("\nFirst table structure:")
        first_table = tables[0]
        table_id = first_table.get_attribute('id')
        table_class = first_table.get_attribute('class')
        print(f"  ID: {table_id}")
        print(f"  Class: {table_class}")

        table_rows = first_table.find_elements(By.TAG_NAME, "tr")
        print(f"  Rows: {len(table_rows)}")

finally:
    driver.quit()
    print("\nDone!")
