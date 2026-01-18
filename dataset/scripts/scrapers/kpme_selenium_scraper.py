"""
KPME Karnataka Portal Scraper with Selenium

Uses Selenium to handle JavaScript-rendered content from KPME portal.
Falls back to creating template CSV if Selenium is not available.
"""

import csv
import logging
from pathlib import Path
from datetime import datetime
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_kpme_template_csv():
    """
    Create KPME_DATA.csv template with structure and sample data

    Note: Full scraping requires:
    1. Selenium WebDriver (for JavaScript rendering)
    2. OR API access to KPME portal
    3. OR manual data export from portal
    """

    logger.info("Creating KPME_DATA.csv template...")

    output_path = Path("dataset/KPME_DATA.csv")

    # Define CSV structure
    headers = [
        'establishment_name',
        'category',
        'system_of_medicine',
        'address',
        'district',
        'taluk',
        'pincode',
        'certificate_number',
        'certificate_validity',
        'phone',
        'email',
        'website',
        'owner_name',
        'registration_date',
        'last_renewed',
        'establishment_type',
        'bed_capacity',
        'specialties',
        'facilities',
        'latitude',
        'longitude',
        'source',
        'scraped_at',
        'data_status'
    ]

    # Sample data based on what we found via WebFetch
    sample_data = [
        {
            'establishment_name': 'SRIHARI DIAGNOSTIC',
            'category': 'Diagnostic Imaging Centre',
            'system_of_medicine': 'Allopathy',
            'address': 'SHOP NO 14, KARAJAGI COMPLEX, OPPOSITE SVM COLLEGE COMPLEX, A/P ILKAL',
            'district': 'BAGALKOTE',
            'taluk': 'ILKAL',
            'pincode': '',
            'certificate_number': 'BGK00035ALDIC',
            'certificate_validity': '20 Oct 2025',
            'phone': '',
            'email': '',
            'website': '',
            'owner_name': '',
            'registration_date': '',
            'last_renewed': '',
            'establishment_type': 'Private',
            'bed_capacity': '',
            'specialties': 'Diagnostic Imaging',
            'facilities': '',
            'latitude': '',
            'longitude': '',
            'source': 'KPME Karnataka Portal',
            'scraped_at': datetime.now().isoformat(),
            'data_status': 'Sample - Extracted from portal via WebFetch'
        },
        {
            'establishment_name': 'Lingaraj Dental Clinic',
            'category': 'Dental Clinic',
            'system_of_medicine': 'Allopathy',
            'address': 'Near Post Office, At:Hirekerur',
            'district': 'HAVERI',
            'taluk': 'HIREKERUR',
            'pincode': '',
            'certificate_number': 'HVR00618ALDEN',
            'certificate_validity': '05 Feb 2029',
            'phone': '',
            'email': '',
            'website': '',
            'owner_name': '',
            'registration_date': '',
            'last_renewed': '',
            'establishment_type': 'Private',
            'bed_capacity': '',
            'specialties': 'Dentistry',
            'facilities': '',
            'latitude': '',
            'longitude': '',
            'source': 'KPME Karnataka Portal',
            'scraped_at': datetime.now().isoformat(),
            'data_status': 'Sample - Extracted from portal via WebFetch'
        },
        {
            'establishment_name': '[TEMPLATE] - Full data requires Selenium/API access',
            'category': 'Hospital/Clinic/Diagnostic Centre/Dental Clinic',
            'system_of_medicine': 'Allopathy/Ayurveda/Homeopathy/Unani/Siddha',
            'address': '[Full address from portal]',
            'district': '[Karnataka District]',
            'taluk': '[Taluk name]',
            'pincode': '[6-digit pincode]',
            'certificate_number': '[KPME Certificate Number]',
            'certificate_validity': '[Validity date]',
            'phone': '[Contact number]',
            'email': '[Email if available]',
            'website': '[Website URL if available]',
            'owner_name': '[Owner/Director name]',
            'registration_date': '[First registration date]',
            'last_renewed': '[Last renewal date]',
            'establishment_type': 'Private/Trust/Charitable',
            'bed_capacity': '[Number of beds]',
            'specialties': '[Medical specialties offered]',
            'facilities': '[Available facilities/services]',
            'latitude': '[GPS latitude]',
            'longitude': '[GPS longitude]',
            'source': 'KPME Karnataka Portal - https://kpme.karnataka.gov.in',
            'scraped_at': datetime.now().isoformat(),
            'data_status': 'TEMPLATE - Replace with actual scraped data'
        }
    ]

    # Write to CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(sample_data)

    logger.info("="*70)
    logger.info("KPME_DATA.csv Created")
    logger.info("="*70)
    logger.info(f"Location: {output_path}")
    logger.info(f"Records: {len(sample_data)} (2 sample + 1 template)")
    logger.info(f"Columns: {len(headers)}")
    logger.info("")
    logger.info("⚠️  NOTE: This is a template with sample data")
    logger.info("")
    logger.info("To collect full KPME data, you need to:")
    logger.info("1. Use Selenium WebDriver to handle JavaScript")
    logger.info("2. OR request API access from KPME portal")
    logger.info("3. OR manually export data from the portal")
    logger.info("")
    logger.info("Portal URLs:")
    logger.info("  - Approved Certs: https://kpme.karnataka.gov.in/AllapplicationList.aspx")
    logger.info("  - Diagnostic Labs: https://kpme.karnataka.gov.in/AllapplicationListLabList.aspx")
    logger.info("="*70)

    return output_path


def scrape_with_selenium():
    """
    Attempt to scrape using Selenium

    Falls back to template if Selenium not available
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.options import Options

        logger.info("Selenium available - attempting to scrape with WebDriver...")

        # Setup Chrome options
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')

        # Initialize driver
        driver = webdriver.Chrome(options=chrome_options)

        establishments = []

        try:
            # Scrape approved certificates
            logger.info("Loading approved certificates page...")
            driver.get("https://kpme.karnataka.gov.in/AllapplicationList.aspx")

            # Wait for table to load
            wait = WebDriverWait(driver, 20)
            table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))

            # Extract data
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header

            logger.info(f"Found {len(rows)} establishments")

            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")

                if len(cells) >= 6:
                    establishment = {
                        'establishment_name': cells[2].text.strip(),
                        'category': cells[1].text.strip(),
                        'system_of_medicine': cells[0].text.strip(),
                        'address': cells[3].text.strip(),
                        'district': '',  # Extract from address
                        'certificate_number': cells[5].text.strip(),
                        'certificate_validity': cells[4].text.strip(),
                        'source': 'KPME Karnataka Portal',
                        'scraped_at': datetime.now().isoformat(),
                        'data_status': 'Scraped via Selenium'
                    }

                    # Extract district from address
                    address_upper = establishment['address'].upper()
                    for district in ['BENGALURU', 'MYSURU', 'MANGALORE', 'HUBLI', 'BELGAUM']:
                        if district in address_upper:
                            establishment['district'] = district
                            break

                    establishments.append(establishment)

            logger.info(f"Successfully scraped {len(establishments)} establishments")

            # Save to CSV
            if establishments:
                output_path = Path("dataset/KPME_DATA.csv")

                import pandas as pd
                df = pd.DataFrame(establishments)
                df.to_csv(output_path, index=False, encoding='utf-8')

                logger.info(f"✓ Saved to {output_path}")
                return output_path

        finally:
            driver.quit()

    except ImportError:
        logger.warning("Selenium not installed - creating template CSV instead")
        logger.info("Install with: pip install selenium")
        return create_kpme_template_csv()

    except Exception as e:
        logger.error(f"Selenium scraping failed: {e}")
        logger.info("Creating template CSV instead...")
        return create_kpme_template_csv()


def main():
    """Main execution"""
    logger.info("KPME Karnataka Portal Data Collection")
    logger.info("="*70)

    # Try Selenium first, fall back to template
    output_path = scrape_with_selenium()

    logger.info("")
    logger.info("✓ KPME data collection completed")
    logger.info(f"  Output: {output_path}")


if __name__ == "__main__":
    main()
