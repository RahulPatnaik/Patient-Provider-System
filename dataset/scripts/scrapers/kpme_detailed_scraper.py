"""
KPME Karnataka Portal - Detailed Establishment Scraper

Scrapes detailed establishment information by clicking "View" buttons
on https://kpme.karnataka.gov.in/AllapplicationList.aspx

Collects comprehensive data for each establishment including:
- System of Medicine, Category, Establishment Name
- Full Address, Certificate Details
- Owner/Contact Information (from detail view)
- All available fields from establishment details popup
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
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KPME_URL = "https://kpme.karnataka.gov.in/AllapplicationList.aspx"
WAIT_TIMEOUT = 20
PAGE_LOAD_DELAY = 3
DETAIL_VIEW_DELAY = 2


class KPMEDetailedScraper:
    """Scraper for detailed KPME establishment data"""

    def __init__(self, headless: bool = False):
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
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.maximize_window()

        logger.info("WebDriver setup complete")

    def extract_table_row_data(self, row) -> Dict[str, Any]:
        """Extract basic data from table row"""
        try:
            cells = row.find_elements(By.TAG_NAME, "td")

            if len(cells) < 8:
                return None

            data = {
                'system_of_medicine': cells[0].text.strip(),
                'category': cells[1].text.strip(),
                'establishment_name': cells[2].text.strip(),
                'address': cells[3].text.strip(),
                'certificate_validity': cells[4].text.strip(),
                'certificate_number': cells[5].text.strip(),
                'rate_details_available': 'View' in cells[6].text,
                'url_details_available': 'View' in cells[7].text if len(cells) > 7 else False,
                'scraped_at': datetime.now().isoformat()
            }

            return data

        except Exception as e:
            logger.warning(f"Error extracting row data: {e}")
            return None

    def click_establishment_details(self, row_index: int) -> Dict[str, Any]:
        """
        Click 'View' button in Establishment Details column and extract popup data

        Args:
            row_index: Index of the row to click

        Returns:
            Dictionary with detailed establishment data
        """
        detailed_data = {}

        try:
            # Find the table again (to avoid stale element reference)
            table = self.driver.find_element(By.ID, "ContentPlaceHolder1_gvw_list")
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header

            if row_index >= len(rows):
                return detailed_data

            row = rows[row_index]

            # Find the "View" button in Establishment Details column (second to last column)
            view_buttons = row.find_elements(By.LINK_TEXT, "View")

            if len(view_buttons) >= 2:  # We want the second View button (Establishment Details)
                establishment_view_button = view_buttons[1]

                # Scroll to element
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", establishment_view_button)
                time.sleep(0.5)

                # Close/hide any overlays
                try:
                    self.driver.execute_script("""
                        // Hide search overlay if present
                        var searchBox = document.querySelector('.gsc-input');
                        if (searchBox && searchBox.parentElement) {
                            searchBox.parentElement.style.display = 'none';
                        }
                    """)
                except:
                    pass

                # Click using JavaScript to bypass overlays
                self.driver.execute_script("arguments[0].click();", establishment_view_button)
                logger.info(f"Clicked establishment details for row {row_index}")

                # Wait for popup/modal to appear
                time.sleep(DETAIL_VIEW_DELAY)

                # Extract data from popup/detail page
                detailed_data = self.extract_detail_popup_data()

                # Close popup/modal if there's a close button
                self.close_popup()

                time.sleep(0.5)

        except Exception as e:
            logger.warning(f"Error clicking establishment details for row {row_index}: {e}")

        return detailed_data

    def extract_detail_popup_data(self) -> Dict[str, Any]:
        """Extract data from the establishment details popup/modal"""
        detail_data = {}

        try:
            # Wait for modal/popup to load
            wait = WebDriverWait(self.driver, 5)

            # Try to find modal or detail container
            # This will depend on the actual structure - adjusting based on what we find

            # Look for common modal/popup patterns
            try:
                modal = wait.until(EC.presence_of_element_located((
                    By.XPATH,
                    "//div[contains(@class, 'modal')]|//div[contains(@class, 'popup')]|//div[contains(@id, 'popup')]"
                )))

                # Extract all text from modal
                modal_text = modal.text

                # Parse the text to extract key-value pairs
                lines = modal_text.split('\n')

                for line in lines:
                    if ':' in line:
                        parts = line.split(':', 1)
                        if len(parts) == 2:
                            key = parts[0].strip().lower().replace(' ', '_')
                            value = parts[1].strip()
                            detail_data[key] = value

                # Try to find specific fields
                try:
                    # Owner name
                    owner_elem = modal.find_element(By.XPATH, ".//*[contains(text(), 'Owner')]")
                    if owner_elem:
                        detail_data['owner_name'] = owner_elem.text.split(':')[-1].strip()
                except:
                    pass

                try:
                    # Contact/Phone
                    phone_elem = modal.find_element(By.XPATH, ".//*[contains(text(), 'Phone') or contains(text(), 'Mobile')]")
                    if phone_elem:
                        detail_data['phone'] = phone_elem.text.split(':')[-1].strip()
                except:
                    pass

                try:
                    # Email
                    email_elem = modal.find_element(By.XPATH, ".//*[contains(text(), 'Email')]")
                    if email_elem:
                        detail_data['email'] = email_elem.text.split(':')[-1].strip()
                except:
                    pass

                try:
                    # Bed capacity
                    beds_elem = modal.find_element(By.XPATH, ".//*[contains(text(), 'Bed') or contains(text(), 'Capacity')]")
                    if beds_elem:
                        detail_data['bed_capacity'] = beds_elem.text.split(':')[-1].strip()
                except:
                    pass

                # Store raw text for analysis
                detail_data['raw_detail_text'] = modal_text[:500]  # First 500 chars

            except TimeoutException:
                logger.warning("No modal/popup found")

        except Exception as e:
            logger.warning(f"Error extracting detail popup data: {e}")

        return detail_data

    def close_popup(self):
        """Close the popup/modal"""
        try:
            # Try common close button patterns
            close_patterns = [
                "//button[contains(text(), 'Close')]",
                "//button[contains(@class, 'close')]",
                "//a[contains(@class, 'close')]",
                "//button[@aria-label='Close']",
                "//span[contains(@class, 'close')]"
            ]

            for pattern in close_patterns:
                try:
                    close_button = self.driver.find_element(By.XPATH, pattern)
                    close_button.click()
                    logger.debug("Popup closed")
                    return
                except:
                    continue

            # If no close button found, try pressing Escape
            from selenium.webdriver.common.keys import Keys
            from selenium.webdriver.common.action_chains import ActionChains
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()

        except Exception as e:
            logger.warning(f"Could not close popup: {e}")

    def scrape_page(self) -> List[Dict[str, Any]]:
        """Scrape all establishments on current page"""
        page_data = []

        try:
            # Wait for table to load
            wait = WebDriverWait(self.driver, WAIT_TIMEOUT)
            table = wait.until(EC.presence_of_element_located((
                By.ID, "ContentPlaceHolder1_gvw_list"
            )))

            # Get all rows (skip header)
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]

            logger.info(f"Found {len(rows)} establishments on this page")

            for idx, row in enumerate(rows):
                try:
                    # Extract basic data from row
                    basic_data = self.extract_table_row_data(row)

                    if not basic_data:
                        continue

                    logger.info(f"Processing {idx+1}/{len(rows)}: {basic_data['establishment_name']}")

                    # Click and get detailed data
                    detailed_data = self.click_establishment_details(idx)

                    # Merge basic and detailed data
                    establishment_data = {**basic_data, **detailed_data}

                    page_data.append(establishment_data)

                    # Rate limiting
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Error processing row {idx}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error scraping page: {e}")

        return page_data

    def has_next_page(self) -> bool:
        """Check if there's a next page button"""
        try:
            # Look for pagination
            next_buttons = self.driver.find_elements(By.XPATH,
                "//a[contains(text(), 'Next')] | //a[contains(@title, 'Next')]"
            )

            if next_buttons:
                # Check if button is enabled
                for button in next_buttons:
                    if button.is_enabled() and button.is_displayed():
                        return True

            return False

        except Exception as e:
            logger.warning(f"Error checking for next page: {e}")
            return False

    def go_to_next_page(self):
        """Click next page button"""
        try:
            next_button = self.driver.find_element(By.XPATH,
                "//a[contains(text(), 'Next')] | //a[contains(@title, 'Next')]"
            )

            if next_button.is_enabled():
                next_button.click()
                time.sleep(PAGE_LOAD_DELAY)
                logger.info("Navigated to next page")
                return True

        except Exception as e:
            logger.error(f"Error going to next page: {e}")

        return False

    def scrape_all_pages(self, max_pages: int = 50) -> List[Dict[str, Any]]:
        """Scrape all pages of establishments"""
        all_data = []
        page_num = 1

        try:
            # Load initial page
            logger.info(f"Loading {KPME_URL}")
            self.driver.get(KPME_URL)
            time.sleep(PAGE_LOAD_DELAY)

            while page_num <= max_pages:
                logger.info(f"Scraping page {page_num}...")

                # Scrape current page
                page_data = self.scrape_page()
                all_data.extend(page_data)

                logger.info(f"Page {page_num}: Collected {len(page_data)} establishments. Total: {len(all_data)}")

                # Check for next page
                if not self.has_next_page():
                    logger.info("No more pages found")
                    break

                # Go to next page
                if not self.go_to_next_page():
                    logger.warning("Could not navigate to next page")
                    break

                page_num += 1

        except Exception as e:
            logger.error(f"Error during scraping: {e}")

        return all_data

    def save_data(self, data: List[Dict[str, Any]]):
        """Save scraped data to CSV and JSON"""
        if not data:
            logger.warning("No data to save")
            return

        # Create output directory
        output_dir = Path("dataset/raw/kpme")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save JSON
        json_path = output_dir / "kpme_detailed_data.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump({
                'scraped_at': datetime.now().isoformat(),
                'total_establishments': len(data),
                'source': 'KPME Karnataka Portal',
                'url': KPME_URL,
                'establishments': data
            }, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved JSON to {json_path}")

        # Save CSV in dataset root
        csv_path = Path("dataset/KPME_DATA.csv")
        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False, encoding='utf-8')

        logger.info(f"✓ Saved CSV to {csv_path}")
        logger.info(f"  Total records: {len(df)}")
        logger.info(f"  Columns: {len(df.columns)}")

    def run(self, max_pages: int = 50):
        """Run the complete scraping process"""
        try:
            logger.info("="*70)
            logger.info("KPME Karnataka - Detailed Establishment Scraper")
            logger.info("="*70)

            # Setup driver
            self.setup_driver()

            # Scrape all pages
            self.establishments = self.scrape_all_pages(max_pages)

            # Save data
            self.save_data(self.establishments)

            logger.info("="*70)
            logger.info(f"✓ Scraping completed successfully!")
            logger.info(f"  Total establishments collected: {len(self.establishments)}")
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
    # Create scraper (set headless=False to see browser)
    scraper = KPMEDetailedScraper(headless=False)

    # Run scraping (max 50 pages, adjust as needed)
    scraper.run(max_pages=50)


if __name__ == "__main__":
    main()
