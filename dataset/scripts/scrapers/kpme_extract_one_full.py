"""
Extract COMPLETE detailed data from ONE KPME establishment
Shows exactly what will be extracted for all establishments
"""

import time
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

KPME_URL = "https://kpme.karnataka.gov.in/AllapplicationList.aspx"

def extract_one_full():
    """Extract complete data from first establishment"""

    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')

    driver = webdriver.Chrome(options=chrome_options)

    try:
        print("Loading KPME portal...")
        driver.get(KPME_URL)
        time.sleep(5)

        main_window = driver.current_window_handle

        # Wait for table
        wait = WebDriverWait(driver, 20)
        table = wait.until(EC.presence_of_element_located((
            By.ID, "ContentPlaceHolder1_gvw_list"
        )))

        rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header
        first_row = rows[0]

        # Extract basic data
        cells = first_row.find_elements(By.TAG_NAME, "td")

        establishment_data = {
            'system_of_medicine': cells[0].text.strip(),
            'category': cells[1].text.strip(),
            'establishment_name': cells[2].text.strip(),
            'address': cells[3].text.strip(),
            'certificate_validity': cells[4].text.strip(),
            'certificate_number': cells[5].text.strip(),
        }

        print("\n" + "=" * 100)
        print(f"EXTRACTING FULL DATA FOR: {establishment_data['establishment_name']}")
        print("=" * 100)

        # Click View button for Establishment Details
        view_buttons = first_row.find_elements(By.LINK_TEXT, "View")
        detail_button = view_buttons[1]  # Second View button

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_button)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", detail_button)

        # Wait for new window
        time.sleep(3)

        # Switch to new window
        all_windows = driver.window_handles
        for window in all_windows:
            if window != main_window:
                driver.switch_to.window(window)
                break

        time.sleep(2)

        # Extract all detailed data from new window
        page_text = driver.find_element(By.TAG_NAME, "body").text

        # Extract specific fields from the page text
        detailed_data = {}

        # GPS Coordinates
        if "Latitude:" in page_text:
            lines = page_text.split('\n')
            for i, line in enumerate(lines):
                if line.strip() == "Latitude:" and i + 1 < len(lines):
                    detailed_data['latitude'] = lines[i + 1].strip()
                elif line.strip() == "Longitude:" and i + 1 < len(lines):
                    detailed_data['longitude'] = lines[i + 1].strip()

        # Contact details
        if "Email:" in page_text:
            lines = page_text.split('\n')
            for i, line in enumerate(lines):
                if line.strip() == "Email:" and i + 1 < len(lines):
                    detailed_data['email'] = lines[i + 1].strip()
                elif line.strip() == "Phone:" and i + 1 < len(lines):
                    detailed_data['phone'] = lines[i + 1].strip()

        # Infrastructure
        if "Land Area(sq.ft):" in page_text:
            lines = page_text.split('\n')
            for i, line in enumerate(lines):
                if line.strip() == "Land Area(sq.ft):" and i + 1 < len(lines):
                    detailed_data['land_area_sqft'] = lines[i + 1].strip()
                elif line.strip() == "Buliding Area(sq.ft):" and i + 1 < len(lines):
                    detailed_data['building_area_sqft'] = lines[i + 1].strip()

        # Number of beds
        if "No of beds:" in page_text:
            lines = page_text.split('\n')
            for i, line in enumerate(lines):
                if line.strip() == "No of beds:" and i + 1 < len(lines):
                    detailed_data['num_beds'] = lines[i + 1].strip()

        # Find all tables
        tables = driver.find_elements(By.TAG_NAME, "table")

        print(f"\n✓ Found {len(tables)} tables in detail window")

        # Extract table data
        table_data = {}

        for idx, table in enumerate(tables):
            rows = table.find_elements(By.TAG_NAME, "tr")

            if len(rows) > 0:
                # Get headers from first row
                header_row = rows[0]
                headers = [cell.text.strip() for cell in header_row.find_elements(By.TAG_NAME, "th")]

                if not headers:
                    headers = [cell.text.strip() for cell in header_row.find_elements(By.TAG_NAME, "td")]

                if headers and any(headers):  # If we have meaningful headers
                    # Extract data rows
                    data_rows = []
                    for row in rows[1:]:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if cells:
                            row_data = [cell.text.strip() for cell in cells]
                            if any(row_data):  # Skip empty rows
                                data_rows.append(row_data)

                    if data_rows:
                        table_name = f"table_{idx + 1}"

                        # Try to identify table by headers
                        header_str = " ".join(headers).lower()

                        if "specialty" in header_str or "specialties" in header_str:
                            table_name = "specialties"
                        elif "staff" in header_str or "professional" in header_str:
                            table_name = "staff_details"
                        elif "surgery" in header_str:
                            table_name = "surgery_fees"
                        elif "consultation" in header_str:
                            table_name = "consultation_fees"
                        elif "treatment" in header_str:
                            table_name = "treatment_charges"
                        elif "administrator" in header_str or "manager" in header_str:
                            table_name = "administrator"
                        elif "attachment" in header_str or "certificate" in header_str:
                            table_name = "certificates"

                        table_data[table_name] = {
                            'headers': headers,
                            'rows': data_rows
                        }

        # Merge all data
        full_data = {
            **establishment_data,
            **detailed_data,
            **table_data
        }

        # Print the extracted data
        print("\n" + "=" * 100)
        print("EXTRACTED DATA:")
        print("=" * 100)

        print("\n📋 BASIC INFORMATION:")
        print(f"  Name: {full_data.get('establishment_name')}")
        print(f"  Category: {full_data.get('category')}")
        print(f"  System: {full_data.get('system_of_medicine')}")
        print(f"  Address: {full_data.get('address')}")
        print(f"  Certificate: {full_data.get('certificate_number')}")
        print(f"  Valid Until: {full_data.get('certificate_validity')}")

        print("\n🌐 GPS & CONTACT:")
        print(f"  Latitude: {full_data.get('latitude', 'N/A')}")
        print(f"  Longitude: {full_data.get('longitude', 'N/A')}")
        print(f"  Email: {full_data.get('email', 'N/A')}")
        print(f"  Phone: {full_data.get('phone', 'N/A')}")

        print("\n🏢 INFRASTRUCTURE:")
        print(f"  Land Area: {full_data.get('land_area_sqft', 'N/A')} sq.ft")
        print(f"  Building Area: {full_data.get('building_area_sqft', 'N/A')} sq.ft")
        print(f"  Number of Beds: {full_data.get('num_beds', 'N/A')}")

        # Print tables
        for table_name, table_info in table_data.items():
            print(f"\n📊 {table_name.upper().replace('_', ' ')}:")
            print(f"  Headers: {table_info['headers']}")
            print(f"  Rows: {len(table_info['rows'])}")
            for i, row in enumerate(table_info['rows'][:3]):  # Show first 3 rows
                print(f"    Row {i + 1}: {row}")
            if len(table_info['rows']) > 3:
                print(f"    ... and {len(table_info['rows']) - 3} more rows")

        # Save to JSON for inspection
        json_output = {}
        for key, value in full_data.items():
            if isinstance(value, dict):
                # For table data, convert to list of dicts
                if 'headers' in value and 'rows' in value:
                    table_list = []
                    for row in value['rows']:
                        row_dict = dict(zip(value['headers'], row))
                        table_list.append(row_dict)
                    json_output[key] = table_list
            else:
                json_output[key] = value

        with open("/tmp/kpme_one_full_extraction.json", "w") as f:
            json.dump(json_output, f, indent=2, ensure_ascii=False)

        print("\n" + "=" * 100)
        print("✅ COMPLETE DATA SAVED TO: /tmp/kpme_one_full_extraction.json")
        print("=" * 100)

        # Close detail window
        driver.close()
        driver.switch_to.window(main_window)

    finally:
        driver.quit()

if __name__ == "__main__":
    extract_one_full()
