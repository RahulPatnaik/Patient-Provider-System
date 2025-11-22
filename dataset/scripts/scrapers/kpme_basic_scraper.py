"""
KPME Karnataka Portal - Basic Data Scraper

Quickly scrapes visible table data without clicking details
Gets: System of Medicine, Category, Name, Address, Certificate Info
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
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KPME_URL = "https://kpme.karnataka.gov.in/AllapplicationList.aspx"

def scrape_kpme_basic():
    """Scrape basic KPME data from table"""

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=chrome_options)
    establishments = []

    try:
        logger.info("Loading KPME portal...")
        driver.get(KPME_URL)
        time.sleep(5)

        page_num = 1
        max_pages = 50

        while page_num <= max_pages:
            logger.info(f"Scraping page {page_num}...")

            # Wait for table
            wait = WebDriverWait(driver, 20)
            table = wait.until(EC.presence_of_element_located((
                By.ID, "ContentPlaceHolder1_gvw_list"
            )))

            # Get rows
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header
            logger.info(f"Found {len(rows)} establishments on page {page_num}")

            for idx, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")

                    if len(cells) >= 6:
                        # Extract visible data
                        establishment = {
                            'system_of_medicine': cells[0].text.strip(),
                            'category': cells[1].text.strip(),
                            'establishment_name': cells[2].text.strip(),
                            'address': cells[3].text.strip(),
                            'certificate_validity': cells[4].text.strip(),
                            'certificate_number': cells[5].text.strip(),
                            'scraped_at': datetime.now().isoformat(),
                            'source': 'KPME Karnataka Portal',
                            'page_number': page_num
                        }

                        # Extract district from address
                        address_upper = establishment['address'].upper()
                        district = ''

                        # Karnataka districts list
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

                        establishment['district'] = district
                        establishments.append(establishment)

                except Exception as e:
                    logger.warning(f"Error extracting row {idx}: {e}")
                    continue

            logger.info(f"Page {page_num}: Extracted {len(rows)} establishments. Total: {len(establishments)}")

            # Check for next page
            try:
                # Try to find the next page number link
                next_page_num = page_num + 1

                # Look for numbered pagination links
                next_page_link = None
                try:
                    # Try direct page number link
                    next_page_link = driver.find_element(By.XPATH,
                        f"//a[text()='{next_page_num}']"
                    )
                except:
                    # Try ellipsis or "..." button for later pages
                    try:
                        ellipsis = driver.find_element(By.XPATH,
                            "//a[contains(text(), '...')]"
                        )
                        ellipsis.click()
                        time.sleep(2)
                        # Try again after clicking ellipsis
                        next_page_link = driver.find_element(By.XPATH,
                            f"//a[text()='{next_page_num}']"
                        )
                    except:
                        pass

                if next_page_link and next_page_link.is_enabled() and next_page_link.is_displayed():
                    # Scroll to element and click
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_page_link)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", next_page_link)
                    time.sleep(3)
                    page_num += 1
                else:
                    logger.info("No more pages available")
                    break

            except Exception as e:
                logger.info(f"Pagination ended: {e}")
                break

    finally:
        driver.quit()

    return establishments


def main():
    logger.info("="*70)
    logger.info("KPME Karnataka - Basic Data Scraper")
    logger.info("="*70)

    # Scrape data
    establishments = scrape_kpme_basic()

    if not establishments:
        logger.warning("No data collected!")
        return

    # Save to CSV
    csv_path = Path("dataset/KPME_DATA.csv")
    df = pd.DataFrame(establishments)

    # Reorder columns
    column_order = [
        'establishment_name', 'category', 'system_of_medicine',
        'address', 'district', 'certificate_number', 'certificate_validity',
        'page_number', 'source', 'scraped_at'
    ]

    df = df[[col for col in column_order if col in df.columns]]
    df.to_csv(csv_path, index=False, encoding='utf-8')

    logger.info("="*70)
    logger.info(f"✓ Scraping completed!")
    logger.info(f"  Total establishments: {len(df)}")
    logger.info(f"  Saved to: {csv_path}")
    logger.info(f"  Columns: {list(df.columns)}")
    logger.info("="*70)

    # Show summary
    print("\nSummary by System of Medicine:")
    print(df['system_of_medicine'].value_counts())
    print("\nSummary by Category:")
    print(df['category'].value_counts().head(10))
    print(f"\nDistricts found: {df['district'].nunique()}")


if __name__ == "__main__":
    main()
