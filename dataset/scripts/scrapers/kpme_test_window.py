"""
Test if clicking View opens a new window/tab
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

KPME_URL = "https://kpme.karnataka.gov.in/AllapplicationList.aspx"

def test_window_popup():
    """Check if clicking View opens a new window"""

    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')

    driver = webdriver.Chrome(options=chrome_options)

    try:
        print("Loading KPME portal...")
        driver.get(KPME_URL)
        time.sleep(5)

        # Get current window handle
        main_window = driver.current_window_handle
        print(f"Main window handle: {main_window}")
        print(f"Total windows before click: {len(driver.window_handles)}")

        # Wait for table
        wait = WebDriverWait(driver, 20)
        table = wait.until(EC.presence_of_element_located((
            By.ID, "ContentPlaceHolder1_gvw_list"
        )))

        rows = table.find_elements(By.TAG_NAME, "tr")[1:]
        first_row = rows[0]

        # Get establishment name
        cells = first_row.find_elements(By.TAG_NAME, "td")
        establishment_name = cells[2].text.strip()
        print(f"\nTesting with: {establishment_name}")

        # Find View buttons
        view_buttons = first_row.find_elements(By.LINK_TEXT, "View")
        print(f"Found {len(view_buttons)} View buttons")

        if len(view_buttons) >= 2:
            detail_button = view_buttons[1]

            # Click the View button
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_button)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", detail_button)
            print("Clicked Establishment Details View button")

            # Wait a bit for any window to open
            time.sleep(3)

            # Check for new windows
            all_windows = driver.window_handles
            print(f"\nTotal windows after click: {len(all_windows)}")

            if len(all_windows) > 1:
                print("✓ NEW WINDOW OPENED!")

                # Switch to the new window
                for window in all_windows:
                    if window != main_window:
                        print(f"Switching to new window: {window}")
                        driver.switch_to.window(window)
                        time.sleep(2)

                        # Now extract data from the new window
                        print("\n" + "=" * 80)
                        print("NEW WINDOW CONTENT:")
                        print("=" * 80)

                        page_text = driver.find_element(By.TAG_NAME, "body").text
                        print(page_text[:3000])  # First 3000 chars

                        # Save the HTML
                        with open("/tmp/kpme_detail_window.html", "w") as f:
                            f.write(driver.page_source)
                        print("\n✓ Saved new window HTML to /tmp/kpme_detail_window.html")

                        # Try to find tables in this window
                        tables = driver.find_elements(By.TAG_NAME, "table")
                        print(f"\n✓ Found {len(tables)} tables in detail window")

                        # Close the popup window
                        driver.close()
                        driver.switch_to.window(main_window)

            else:
                print("✗ No new window opened - data might be in modal/iframe")

                # Let's check the main page more carefully
                print("\nChecking for modal dialog...")

                # Wait a bit more
                time.sleep(2)

                # Try different methods to find the popup content
                try:
                    # Look for any recently added div (ASP.NET modals often use specific IDs)
                    popup_divs = driver.find_elements(By.XPATH,
                        "//div[contains(@style, 'display: block') or contains(@style, 'visibility: visible')]")

                    print(f"Found {len(popup_divs)} visible divs")

                    for idx, div in enumerate(popup_divs[:10]):
                        div_id = div.get_attribute('id')
                        div_class = div.get_attribute('class')
                        if div_id or div_class:
                            print(f"  Div {idx}: id='{div_id}', class='{div_class}'")
                            text = div.text[:200] if div.text else "(empty)"
                            print(f"    Text: {text}")

                except Exception as e:
                    print(f"Error: {e}")

    finally:
        time.sleep(2)
        driver.quit()
        print("\nTest completed!")

if __name__ == "__main__":
    test_window_popup()
