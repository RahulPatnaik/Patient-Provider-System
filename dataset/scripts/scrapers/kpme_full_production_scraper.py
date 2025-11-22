"""
KPME Karnataka - Full Production Scraper with Multi-threading

Features:
- Headless browser (runs in background)
- Multi-threaded page scraping (4 threads)
- Extracts ALL detail data from View popups
- Generates normalized tables (5 CSV files)
- Progress tracking and error handling
"""

import time
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KPME_URL = "https://kpme.karnataka.gov.in/AllapplicationList.aspx"
HEADLESS = True  # Run browser in background
NUM_THREADS = 4  # Number of concurrent page scrapers

# Thread-safe counters
lock = threading.Lock()
total_extracted = 0

class KPMEFullScraper:
    """Multi-threaded KPME scraper with full detail extraction"""

    def __init__(self):
        self.establishments_data = []
        self.staff_data = []
        self.specialties_data = []
        self.certificates_data = []
        self.treatments_data = []
        self.establishment_id_counter = 0

    def setup_driver(self):
        """Setup headless Chrome WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Run in background
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-software-rasterizer')

        driver = webdriver.Chrome(options=chrome_options)
        return driver

    def get_next_id(self):
        """Thread-safe ID generator"""
        with lock:
            self.establishment_id_counter += 1
            return self.establishment_id_counter

    def extract_detail_data(self, driver, main_window) -> Dict[str, Any]:
        """Extract all detailed data from new window"""
        detailed_data = {}

        try:
            # Wait for new window
            time.sleep(2)

            # Switch to detail window
            all_windows = driver.window_handles
            for window in all_windows:
                if window != main_window:
                    driver.switch_to.window(window)
                    break

            time.sleep(1)

            # Get page text
            page_text = driver.find_element(By.TAG_NAME, "body").text

            # Extract GPS coordinates
            lines = page_text.split('\n')
            for i, line in enumerate(lines):
                if line.strip() == "Latitude:" and i + 1 < len(lines):
                    detailed_data['latitude'] = lines[i + 1].strip()
                elif line.strip() == "Longitude:" and i + 1 < len(lines):
                    detailed_data['longitude'] = lines[i + 1].strip()
                elif line.strip() == "Email:" and i + 1 < len(lines):
                    detailed_data['email'] = lines[i + 1].strip()
                elif line.strip() == "Phone:" and i + 1 < len(lines):
                    detailed_data['phone'] = lines[i + 1].strip()
                elif line.strip() == "Land Area(sq.ft):" and i + 1 < len(lines):
                    detailed_data['land_area_sqft'] = lines[i + 1].strip()
                elif line.strip() == "Buliding Area(sq.ft):" and i + 1 < len(lines):
                    detailed_data['building_area_sqft'] = lines[i + 1].strip()
                elif line.strip() == "No of beds:" and i + 1 < len(lines):
                    detailed_data['num_beds'] = lines[i + 1].strip()

            # Extract tables
            tables = driver.find_elements(By.TAG_NAME, "table")
            table_data = {}

            for table in tables:
                rows = table.find_elements(By.TAG_NAME, "tr")
                if len(rows) == 0:
                    continue

                # Get headers
                header_row = rows[0]
                headers = [cell.text.strip() for cell in header_row.find_elements(By.TAG_NAME, "th")]
                if not headers:
                    headers = [cell.text.strip() for cell in header_row.find_elements(By.TAG_NAME, "td")]

                if not headers or not any(headers):
                    continue

                # Extract data rows
                data_rows = []
                for row in rows[1:]:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if cells:
                        row_data = [cell.text.strip() for cell in cells]
                        if any(row_data):
                            data_rows.append(row_data)

                if data_rows:
                    header_str = " ".join(headers).lower()

                    # Identify table type
                    if "specialty" in header_str or "specialties" in header_str:
                        table_data['specialties'] = {'headers': headers, 'rows': data_rows}
                    elif "staff" in header_str or "professional" in header_str and "consultation" in header_str:
                        table_data['consultation_fees'] = {'headers': headers, 'rows': data_rows}
                    elif "surgery" in header_str:
                        table_data['surgery_fees'] = {'headers': headers, 'rows': data_rows}
                    elif "treatment" in header_str:
                        table_data['treatment_charges'] = {'headers': headers, 'rows': data_rows}
                    elif "administrator" in header_str or "manager" in header_str:
                        table_data['administrator'] = {'headers': headers, 'rows': data_rows}
                    elif "attachment" in header_str or "certificate" in header_str or "expiry" in header_str:
                        table_data['certificates'] = {'headers': headers, 'rows': data_rows}
                    elif "registration" in header_str and "name" in header_str and "qualification" in header_str:
                        table_data['staff_details'] = {'headers': headers, 'rows': data_rows}

            detailed_data['tables'] = table_data

            # Close detail window
            driver.close()
            driver.switch_to.window(main_window)

        except Exception as e:
            logger.error(f"Error extracting detail data: {e}")
            try:
                driver.switch_to.window(main_window)
            except:
                pass

        return detailed_data

    def scrape_establishment(self, driver, row, row_index, main_window, page_num):
        """Scrape one establishment including detail data"""
        global total_extracted

        try:
            # Extract basic data
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 6:
                return None

            establishment_id = self.get_next_id()

            basic_data = {
                'id': establishment_id,
                'system_of_medicine': cells[0].text.strip(),
                'category': cells[1].text.strip(),
                'establishment_name': cells[2].text.strip(),
                'address': cells[3].text.strip(),
                'certificate_validity': cells[4].text.strip(),
                'certificate_number': cells[5].text.strip(),
                'page_number': page_num,
            }

            # Extract district from address
            address_upper = basic_data['address'].upper()
            districts = [
                'BAGALKOTE', 'BALLARI', 'BELAGAVI', 'BENGALURU', 'BIDAR',
                'CHAMARAJANAGARA', 'CHIKKABALLAPURA', 'CHIKKAMAGALURU', 'CHITRADURGA',
                'DAKSHINA KANNADA', 'DAVANAGERE', 'DHARWAD', 'GADAG', 'HASSAN',
                'HAVERI', 'KALABURAGI', 'KODAGU', 'KOLAR', 'KOPPAL', 'MANDYA',
                'MYSURU', 'MYSORE', 'RAICHUR', 'RAMANAGARA', 'SHIVAMOGGA', 'TUMAKURU',
                'UDUPI', 'UTTARA KANNADA', 'VIJAYAPURA', 'YADGIR', 'BANGALORE',
                'MANGALORE', 'HUBLI', 'BELGAUM'
            ]

            district = ''
            for dist in districts:
                if dist in address_upper:
                    district = dist
                    break
            basic_data['district'] = district

            # Click View button for details
            view_buttons = row.find_elements(By.LINK_TEXT, "View")
            if len(view_buttons) >= 2:
                detail_button = view_buttons[1]

                # Click
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_button)
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", detail_button)

                # Extract detailed data
                detailed_data = self.extract_detail_data(driver, main_window)

                # Merge basic and detailed data
                full_data = {**basic_data, **detailed_data}

                # Store in appropriate lists
                self.store_normalized_data(full_data)

                with lock:
                    total_extracted += 1

                logger.info(f"✓ [{total_extracted}/1020] {basic_data['establishment_name'][:50]}")

                return full_data
            else:
                logger.warning(f"Not enough View buttons for: {basic_data['establishment_name']}")
                return basic_data

        except Exception as e:
            logger.error(f"Error scraping row {row_index}: {e}")
            return None

    def store_normalized_data(self, data: Dict[str, Any]):
        """Store data in normalized tables"""
        est_id = data['id']
        est_name = data['establishment_name']

        # Main establishment data
        main_row = {
            'id': est_id,
            'establishment_name': est_name,
            'category': data.get('category', ''),
            'system_of_medicine': data.get('system_of_medicine', ''),
            'address': data.get('address', ''),
            'district': data.get('district', ''),
            'certificate_number': data.get('certificate_number', ''),
            'certificate_validity': data.get('certificate_validity', ''),
            'latitude': data.get('latitude', ''),
            'longitude': data.get('longitude', ''),
            'email': data.get('email', ''),
            'phone': data.get('phone', ''),
            'num_beds': data.get('num_beds', ''),
            'land_area_sqft': data.get('land_area_sqft', ''),
            'building_area_sqft': data.get('building_area_sqft', ''),
            'page_number': data.get('page_number', ''),
        }

        with lock:
            self.establishments_data.append(main_row)

        # Extract table data
        tables = data.get('tables', {})

        # Specialties
        if 'specialties' in tables:
            for row in tables['specialties']['rows']:
                if len(row) >= 2:
                    with lock:
                        self.specialties_data.append({
                            'establishment_id': est_id,
                            'establishment_name': est_name,
                            'specialty': row[0],
                            'type': row[1]
                        })

        # Staff details and fees
        staff_details = tables.get('staff_details', {}).get('rows', [])
        consultation_fees = tables.get('consultation_fees', {}).get('rows', [])
        surgery_fees = tables.get('surgery_fees', {}).get('rows', [])

        # Build staff dictionary by registration number
        staff_dict = {}

        # From staff_details table
        for row in staff_details:
            if len(row) >= 5:
                reg_no = row[1] if len(row) > 1 else ''
                staff_dict[reg_no] = {
                    'registration_no': reg_no,
                    'name': row[2] if len(row) > 2 else '',
                    'qualification': row[3] if len(row) > 3 else '',
                    'job_type': row[4] if len(row) > 4 else '',
                    'mobile': row[5] if len(row) > 5 else '',
                    'consultation_fee': '',
                    'surgery_fee': ''
                }

        # Add consultation fees
        for row in consultation_fees:
            if len(row) >= 3:
                reg_no = row[1] if len(row) > 1 else ''
                if reg_no in staff_dict:
                    staff_dict[reg_no]['consultation_fee'] = row[3] if len(row) > 3 else ''
                else:
                    staff_dict[reg_no] = {
                        'registration_no': reg_no,
                        'name': row[2] if len(row) > 2 else '',
                        'qualification': '',
                        'job_type': '',
                        'mobile': '',
                        'consultation_fee': row[3] if len(row) > 3 else '',
                        'surgery_fee': ''
                    }

        # Add surgery fees
        for row in surgery_fees:
            if len(row) >= 3:
                reg_no = row[1] if len(row) > 1 else ''
                if reg_no in staff_dict:
                    staff_dict[reg_no]['surgery_fee'] = row[3] if len(row) > 3 else ''

        # Store staff data
        for staff in staff_dict.values():
            with lock:
                self.staff_data.append({
                    'establishment_id': est_id,
                    'establishment_name': est_name,
                    **staff
                })

        # Certificates
        if 'certificates' in tables:
            for row in tables['certificates']['rows']:
                if len(row) >= 2:
                    with lock:
                        self.certificates_data.append({
                            'establishment_id': est_id,
                            'establishment_name': est_name,
                            'certificate_name': row[0],
                            'expiry_date': row[1]
                        })

        # Treatment charges
        if 'treatment_charges' in tables:
            for row in tables['treatment_charges']['rows']:
                if len(row) >= 3:
                    with lock:
                        self.treatments_data.append({
                            'establishment_id': est_id,
                            'establishment_name': est_name,
                            'treatment_name': row[1] if len(row) > 1 else '',
                            'treatment_code': row[2] if len(row) > 2 else '',
                            'amount': row[3] if len(row) > 3 else ''
                        })

    def scrape_page_sequential(self, page_num: int):
        """Scrape one page sequentially (each page in its own thread)"""
        driver = self.setup_driver()

        try:
            logger.info(f"Starting page {page_num}")

            # Navigate to KPME portal
            driver.get(KPME_URL)
            time.sleep(5)

            main_window = driver.current_window_handle

            # Navigate to specific page
            if page_num > 1:
                try:
                    page_link = driver.find_element(By.XPATH, f"//a[text()='{page_num}']")
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", page_link)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", page_link)
                    time.sleep(3)
                except Exception as e:
                    logger.error(f"Could not navigate to page {page_num}: {e}")
                    return

            # Wait for table
            wait = WebDriverWait(driver, 20)
            table = wait.until(EC.presence_of_element_located((
                By.ID, "ContentPlaceHolder1_gvw_list"
            )))

            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header
            logger.info(f"Page {page_num}: Found {len(rows)} establishments")

            # Scrape each row
            for idx in range(len(rows)):
                # Re-find table and rows to avoid stale elements
                table = driver.find_element(By.ID, "ContentPlaceHolder1_gvw_list")
                rows = table.find_elements(By.TAG_NAME, "tr")[1:]

                if idx >= len(rows):
                    continue

                self.scrape_establishment(driver, rows[idx], idx, main_window, page_num)
                time.sleep(0.5)  # Small delay between establishments

        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}")

        finally:
            driver.quit()
            logger.info(f"Completed page {page_num}")

    def scrape_all_pages_multithreaded(self, max_pages: int = 10):
        """Scrape all pages using multi-threading"""
        logger.info(f"Starting multi-threaded scraping with {NUM_THREADS} threads")

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = [executor.submit(self.scrape_page_sequential, page_num)
                      for page_num in range(1, max_pages + 1)]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Page scraping failed: {e}")

    def save_data(self):
        """Save all data to normalized CSV files"""
        output_dir = Path("dataset")

        logger.info("\n" + "=" * 70)
        logger.info("SAVING DATA TO CSV FILES")
        logger.info("=" * 70)

        # Main establishments
        if self.establishments_data:
            df_main = pd.DataFrame(self.establishments_data)
            df_main = df_main.sort_values('id')
            df_main.to_csv(output_dir / "KPME_FULL_DATA.csv", index=False)
            logger.info(f"✓ Main: {len(df_main)} establishments -> KPME_FULL_DATA.csv")

        # Staff
        if self.staff_data:
            df_staff = pd.DataFrame(self.staff_data)
            df_staff.to_csv(output_dir / "KPME_STAFF.csv", index=False)
            logger.info(f"✓ Staff: {len(df_staff)} records -> KPME_STAFF.csv")

        # Specialties
        if self.specialties_data:
            df_spec = pd.DataFrame(self.specialties_data)
            df_spec.to_csv(output_dir / "KPME_SPECIALTIES.csv", index=False)
            logger.info(f"✓ Specialties: {len(df_spec)} records -> KPME_SPECIALTIES.csv")

        # Certificates
        if self.certificates_data:
            df_cert = pd.DataFrame(self.certificates_data)
            df_cert.to_csv(output_dir / "KPME_CERTIFICATES.csv", index=False)
            logger.info(f"✓ Certificates: {len(df_cert)} records -> KPME_CERTIFICATES.csv")

        # Treatments
        if self.treatments_data:
            df_treat = pd.DataFrame(self.treatments_data)
            df_treat.to_csv(output_dir / "KPME_TREATMENTS.csv", index=False)
            logger.info(f"✓ Treatments: {len(df_treat)} records -> KPME_TREATMENTS.csv")

        logger.info("=" * 70)

    def run(self, max_pages: int = 10):
        """Run the complete scraping process"""
        try:
            logger.info("=" * 70)
            logger.info("KPME FULL PRODUCTION SCRAPER - MULTI-THREADED")
            logger.info("=" * 70)
            logger.info(f"Headless mode: {HEADLESS}")
            logger.info(f"Threads: {NUM_THREADS}")
            logger.info(f"Pages: {max_pages}")
            logger.info("=" * 70)

            start_time = time.time()

            # Scrape all pages
            self.scrape_all_pages_multithreaded(max_pages)

            # Save data
            self.save_data()

            elapsed = time.time() - start_time
            logger.info(f"\n✓ SCRAPING COMPLETED in {elapsed/60:.1f} minutes")
            logger.info(f"  Total establishments: {len(self.establishments_data)}")

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise


def main():
    """Main execution"""
    scraper = KPMEFullScraper()
    scraper.run(max_pages=10)


if __name__ == "__main__":
    main()
