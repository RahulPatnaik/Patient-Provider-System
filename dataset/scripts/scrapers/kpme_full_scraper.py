"""
KPME Karnataka Portal - Full Data Scraper with Detail Extraction

Scrapes ALL data including:
- Basic table data (name, category, address, certificate)
- Detailed establishment information from "View" popup
- All certificates, owner info, contact details, bed capacity, etc.
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
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KPME_URL = "https://kpme.karnataka.gov.in/AllapplicationList.aspx"

class KPMEFullScraper:
    """Complete KPME scraper with detail popup extraction"""

    def __init__(self, headless=True):
        self.headless = headless
        self.driver = None
        self.establishments = []

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        chrome_options = Options()

        if self.headless:
            chrome_options.add_argument('--headless')

        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')

        self.driver = webdriver.Chrome(options=chrome_options)
        logger.info("WebDriver setup complete")

    def extract_basic_data(self, row) -> Dict[str, Any]:
        """Extract basic data from table row"""
        try:
            cells = row.find_elements(By.TAG_NAME, "td")

            if len(cells) < 6:
                return None

            data = {
                'system_of_medicine': cells[0].text.strip(),
                'category': cells[1].text.strip(),
                'establishment_name': cells[2].text.strip(),
                'address': cells[3].text.strip(),
                'certificate_validity': cells[4].text.strip(),
                'certificate_number': cells[5].text.strip(),
            }

            # Extract district from address
            address_upper = data['address'].upper()
            district = ''

            districts = [
                'BAGALKOTE', 'BALLARI', 'BELAGAVI', 'BENGALURU', 'BIDAR',
                'CHAMARAJANAGARA', 'CHIKKABALLAPURA', 'CHIKKAMAGALURU', 'CHITRADURGA',
                'DAKSHINA KANNADA', 'DAVANAGERE', 'DHARWAD', 'GADAG', 'HASSAN',
                'HAVERI', 'KALABURAGI', 'KODAGU', 'KOLAR', 'KOPPAL', 'MANDYA',
                'MYSURU', 'MYSORE', 'RAICHUR', 'RAMANAGARA', 'SHIVAMOGGA', 'TUMAKURU',
                'UDUPI', 'UTTARA KANNADA', 'VIJAYAPURA', 'YADGIR', 'BANGALORE',
                'MANGALORE', 'HUBLI', 'BELGAUM'
            ]

            for dist in districts:
                if dist in address_upper:
                    district = dist
                    break

            data['district'] = district

            return data

        except Exception as e:
            logger.warning(f"Error extracting basic data: {e}")
            return None

    def click_and_extract_details(self, row_index: int) -> Dict[str, Any]:
        """
        Click 'View' button in Establishment Details column and extract all popup data

        Returns detailed information including:
        - Owner name, contact details
        - Additional certificates
        - Bed capacity, facility details
        - Registration info
        - All other fields from the detail popup
        """
        detailed_data = {}

        try:
            # Re-find table to avoid stale element
            wait = WebDriverWait(self.driver, 10)
            table = wait.until(EC.presence_of_element_located((
                By.ID, "ContentPlaceHolder1_gvw_list"
            )))

            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header

            if row_index >= len(rows):
                logger.warning(f"Row index {row_index} out of range")
                return detailed_data

            row = rows[row_index]

            # Find all "View" buttons in the row
            # The Establishment Details "View" is typically in the 8th column (index 7)
            view_buttons = row.find_elements(By.LINK_TEXT, "View")

            if len(view_buttons) < 2:
                logger.warning(f"Not enough View buttons found in row {row_index}")
                return detailed_data

            # The second "View" button is for Establishment Details
            detail_view_button = view_buttons[1]

            # Scroll to element
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_view_button)
            time.sleep(0.3)

            # Click using JavaScript to bypass any overlays
            self.driver.execute_script("arguments[0].click();", detail_view_button)
            logger.debug(f"Clicked detail view for row {row_index}")

            # Wait for popup/modal to load
            time.sleep(2)

            # Extract all data from popup
            detailed_data = self.extract_popup_data()

            # Close popup
            self.close_popup()

            time.sleep(0.3)

        except Exception as e:
            logger.warning(f"Error clicking details for row {row_index}: {e}")

        return detailed_data

    def extract_popup_data(self) -> Dict[str, Any]:
        """Extract all data from the establishment details popup/modal"""
        detail_data = {}

        try:
            wait = WebDriverWait(self.driver, 5)

            # Try to find the modal/popup container
            modal = None
            try:
                # Try common modal patterns
                modal = wait.until(EC.presence_of_element_located((
                    By.XPATH,
                    "//div[contains(@class, 'modal')]|//div[contains(@class, 'popup')]|//div[contains(@id, 'modal')]|//div[contains(@id, 'popup')]"
                )))
            except TimeoutException:
                # Try to find any dialog or panel
                try:
                    modal = self.driver.find_element(By.XPATH,
                        "//div[@role='dialog']|//div[contains(@class, 'dialog')]|//div[contains(@class, 'panel')]"
                    )
                except:
                    logger.warning("Could not find modal/popup container")
                    return detail_data

            if modal:
                # Get all text content from modal
                modal_text = modal.text
                detail_data['raw_popup_text'] = modal_text

                # Try to find structured data in tables within the modal
                try:
                    detail_tables = modal.find_elements(By.TAG_NAME, "table")

                    for table in detail_tables:
                        rows = table.find_elements(By.TAG_NAME, "tr")

                        for row in rows:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 2:
                                # Assume first cell is label, second is value
                                label = cells[0].text.strip().lower().replace(' ', '_').replace(':', '')
                                value = cells[1].text.strip()

                                if label and value:
                                    detail_data[f'detail_{label}'] = value
                except Exception as e:
                    logger.debug(f"Error extracting table data: {e}")

                # Try to parse key-value pairs from text
                lines = modal_text.split('\n')
                for line in lines:
                    if ':' in line:
                        parts = line.split(':', 1)
                        if len(parts) == 2:
                            key = parts[0].strip().lower().replace(' ', '_').replace('/', '_')
                            value = parts[1].strip()
                            if key and value:
                                detail_data[f'popup_{key}'] = value

                # Look for specific common fields
                field_patterns = {
                    'owner_name': ['Owner', 'Proprietor', 'Managing Director'],
                    'contact_person': ['Contact Person', 'Contact Name'],
                    'phone': ['Phone', 'Mobile', 'Contact No', 'Telephone'],
                    'email': ['Email', 'E-mail', 'Email ID'],
                    'bed_capacity': ['Bed', 'Beds', 'Bed Capacity', 'No of Beds'],
                    'registration_number': ['Registration No', 'Reg No', 'Registration Number'],
                    'registration_date': ['Registration Date', 'Reg Date'],
                    'facility_type': ['Facility Type', 'Type of Facility'],
                    'services': ['Services', 'Services Provided'],
                    'specialities': ['Specialities', 'Specialties', 'Specialization'],
                }

                for field_key, patterns in field_patterns.items():
                    for pattern in patterns:
                        try:
                            elem = modal.find_element(By.XPATH,
                                f".//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{pattern.lower()}')]"
                            )
                            parent = elem.find_element(By.XPATH, "..")
                            text = parent.text

                            if ':' in text:
                                value = text.split(':', 1)[1].strip()
                                if value:
                                    detail_data[field_key] = value
                                    break
                        except:
                            continue

        except Exception as e:
            logger.warning(f"Error extracting popup data: {e}")

        return detail_data

    def close_popup(self):
        """Close the popup/modal"""
        try:
            # Try common close button patterns
            close_patterns = [
                "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'close')]",
                "//button[contains(@class, 'close')]",
                "//a[contains(@class, 'close')]",
                "//button[@aria-label='Close']",
                "//span[contains(@class, 'close')]",
                "//button[contains(@class, 'btn-close')]",
                "//i[contains(@class, 'close')]/..",
            ]

            for pattern in close_patterns:
                try:
                    close_button = self.driver.find_element(By.XPATH, pattern)
                    if close_button.is_displayed():
                        self.driver.execute_script("arguments[0].click();", close_button)
                        logger.debug("Popup closed")
                        time.sleep(0.3)
                        return
                except:
                    continue

            # If no close button found, try pressing Escape
            from selenium.webdriver.common.keys import Keys
            from selenium.webdriver.common.action_chains import ActionChains
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.3)

        except Exception as e:
            logger.debug(f"Could not close popup: {e}")

    def scrape_page(self, page_num: int) -> List[Dict[str, Any]]:
        """Scrape all establishments on current page with details"""
        page_data = []

        try:
            wait = WebDriverWait(self.driver, 20)
            table = wait.until(EC.presence_of_element_located((
                By.ID, "ContentPlaceHolder1_gvw_list"
            )))

            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header
            total_rows = len(rows)

            logger.info(f"Found {total_rows} establishments on page {page_num}")

            for idx in range(total_rows):
                try:
                    # Re-find table and rows for each iteration (avoid stale elements)
                    table = self.driver.find_element(By.ID, "ContentPlaceHolder1_gvw_list")
                    rows = table.find_elements(By.TAG_NAME, "tr")[1:]

                    if idx >= len(rows):
                        logger.warning(f"Row {idx} disappeared")
                        continue

                    row = rows[idx]

                    # Extract basic data
                    basic_data = self.extract_basic_data(row)

                    if not basic_data:
                        continue

                    logger.info(f"  [{idx+1}/{total_rows}] {basic_data['establishment_name'][:50]}")

                    # Click and extract detailed data
                    detailed_data = self.click_and_extract_details(idx)

                    # Merge all data
                    full_data = {
                        **basic_data,
                        **detailed_data,
                        'page_number': page_num,
                        'source': 'KPME Karnataka Portal',
                        'scraped_at': datetime.now().isoformat(),
                    }

                    page_data.append(full_data)

                    # Small delay between rows
                    time.sleep(0.5)

                except Exception as e:
                    logger.error(f"Error processing row {idx}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error scraping page: {e}")

        return page_data

    def navigate_to_page(self, page_num: int) -> bool:
        """Navigate to specific page number"""
        try:
            # Find and click page number link
            page_link = self.driver.find_element(By.XPATH, f"//a[text()='{page_num}']")

            if page_link.is_enabled() and page_link.is_displayed():
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", page_link)
                time.sleep(0.5)
                self.driver.execute_script("arguments[0].click();", page_link)
                time.sleep(3)
                return True

        except Exception as e:
            logger.warning(f"Could not navigate to page {page_num}: {e}")

        return False

    def scrape_all_pages(self, max_pages: int = 10) -> List[Dict[str, Any]]:
        """Scrape all pages with full details"""
        all_data = []

        try:
            logger.info(f"Loading {KPME_URL}")
            self.driver.get(KPME_URL)
            time.sleep(5)

            for page_num in range(1, max_pages + 1):
                logger.info(f"\n{'='*70}")
                logger.info(f"SCRAPING PAGE {page_num}")
                logger.info(f"{'='*70}")

                # Navigate to page (skip navigation for page 1)
                if page_num > 1:
                    if not self.navigate_to_page(page_num):
                        logger.warning(f"Could not navigate to page {page_num}, stopping")
                        break

                # Scrape current page
                page_data = self.scrape_page(page_num)
                all_data.extend(page_data)

                logger.info(f"Page {page_num}: Collected {len(page_data)} establishments. Total: {len(all_data)}")

                # Check if we can continue
                try:
                    next_page = page_num + 1
                    next_link = self.driver.find_element(By.XPATH, f"//a[text()='{next_page}']")
                    if not (next_link.is_enabled() and next_link.is_displayed()):
                        logger.info("No more pages available")
                        break
                except:
                    if page_num >= max_pages:
                        logger.info(f"Reached max pages ({max_pages})")
                    else:
                        logger.info("No more pages available")
                    break

        except Exception as e:
            logger.error(f"Error during scraping: {e}")

        return all_data

    def save_data(self, data: List[Dict[str, Any]]):
        """Save data to CSV"""
        if not data:
            logger.warning("No data to save")
            return

        # Save full detailed CSV
        csv_path = Path("dataset/KPME_FULL_DATA.csv")
        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False, encoding='utf-8')

        logger.info(f"\n{'='*70}")
        logger.info(f"✓ Saved full data to: {csv_path}")
        logger.info(f"  Total records: {len(df):,}")
        logger.info(f"  Total columns: {len(df.columns)}")
        logger.info(f"  Columns: {list(df.columns)}")
        logger.info(f"{'='*70}")

    def run(self, max_pages: int = 10):
        """Run the complete scraping process"""
        try:
            logger.info("="*70)
            logger.info("KPME Karnataka - Full Data Scraper with Details")
            logger.info("="*70)

            self.setup_driver()
            self.establishments = self.scrape_all_pages(max_pages)
            self.save_data(self.establishments)

            logger.info("\n" + "="*70)
            logger.info(f"✓ Scraping completed!")
            logger.info(f"  Total establishments: {len(self.establishments):,}")
            logger.info("="*70)

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise

        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed")


def main():
    """Main execution"""
    scraper = KPMEFullScraper(headless=True)
    scraper.run(max_pages=10)


if __name__ == "__main__":
    main()
