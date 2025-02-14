import requests
import re
import pandas as pd
from time import sleep
from typing import Tuple, Optional, List, Dict
import logging
from urllib.parse import urljoin
from requests.exceptions import RequestException
import string
from pymongo import MongoClient, ASCENDING
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MongoDBHandler:
    def __init__(self, connection_string: str = "mongodb://localhost:27017/",database_name='profession_lead'):
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db.Arkansas_medical_board_profiles2
    
        
        # Create index on Phone field
        self.collection.create_index([("Phone", ASCENDING)], unique=True)
        
    def insert_profile(self, profile: Dict) -> bool:
        """Insert a single profile into MongoDB with upsert logic"""
        try:
            # Check if phone number is valid
            phone = profile.get('Phone', '').strip()
            if phone in ['Not available', '', 'nan', None] or len(phone) < 10:
                logger.warning(f"Skipping profile for {profile['Name']} - Invalid phone number: {phone}")
                return False
                
            # Add timestamp for tracking
            profile['last_updated'] = datetime.utcnow()
            
            # Upsert based on Phone number
            self.collection.update_one(
                {"Phone": profile["Phone"]},
                {"$set": profile},
                upsert=True
            )
            logger.info(f"Successfully upserted profile for {profile['Name']} with phone {phone}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert profile for {profile['Name']}: {str(e)}")
            return False

class MedicalBoardScraper:
    BASE_URL = "https://www.armedicalboard.org"
    VERIFY_URL = urljoin(BASE_URL, "/public/verify/default.aspx")
    LOOKUP_URL = urljoin(BASE_URL, "/Public/verify/lookup.aspx")
    RESULTS_URL = urljoin(BASE_URL, "/Public/verify/results.aspx")

    def __init__(self, request_delay: float = 1.5):
        self.session = requests.Session()
        self.request_delay = request_delay
        self.headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.mongo_handler = MongoDBHandler()
        self.stats = {
            'total_scraped': 0,
            'valid_profiles': 0,
            'invalid_phone': 0
        }

    def fetch_initial_state(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Fetch the initial state required for making POST requests."""
        try:
            response = self.session.get(self.VERIFY_URL, headers=self.headers)
            response.raise_for_status()
            
            patterns = {
                'viewstate': r'id="__VIEWSTATE" value="([^"]+)"',
                'eventvalidation': r'id="__EVENTVALIDATION" value="([^"]+)"',
                'viewstategenerator': r'id="__VIEWSTATEGENERATOR" value="([^"]+)"'
            }
            
            results = {key: re.search(pattern, response.text) 
                      for key, pattern in patterns.items()}
            
            return (results['viewstate'].group(1) if results['viewstate'] else None,
                    results['eventvalidation'].group(1) if results['eventvalidation'] else None,
                    results['viewstategenerator'].group(1) if results['viewstategenerator'] else None)
                    
        except RequestException as e:
            logger.error(f"Failed to fetch initial state: {str(e)}")
            return None, None, None

    def search_profiles_by_last_name(self, last_name: str, page_num: int = 1) -> Optional[str]:
        """Fetch the list of profiles based on the last name and page number."""
        try:
            viewstate, eventvalidation, viewstategenerator = self.fetch_initial_state()
            if not all([viewstate, eventvalidation, viewstategenerator]):
                raise ValueError("Failed to fetch required form values")

            data = {
                '__VIEWSTATE': viewstate,
                '__EVENTVALIDATION': eventvalidation,
                '__VIEWSTATEGENERATOR': viewstategenerator,
                '__EVENTTARGET': '',
                '__EVENTARGUMENT': '',
                'ctl00$MainContentPlaceHolder$ucVerifyLicense$txtVerifyLicNumLastName': last_name,
                'ctl00$MainContentPlaceHolder$ucVerifyLicense$btnVerifyLicense': 'Verify'
            }

            response = self.session.post(
                self.VERIFY_URL,
                headers=self.headers,
                data=data
            )
            response.raise_for_status()

            results_url = f"{self.LOOKUP_URL}?LName={last_name}&page={page_num}"
            result_response = self.session.get(results_url, headers=self.headers)
            result_response.raise_for_status()
            
            return result_response.text

        except (RequestException, ValueError) as e:
            logger.error(f"Error searching profiles for {last_name} on page {page_num}: {str(e)}")
            return None
        
    

    @staticmethod
    def extract_profile_links(html_content: str) -> List[str]:
        """Extract all profile links from a search results page."""
        pattern = r'href="results\.aspx\?strPHIDNO=(ASMB\d+)"'
        matches = re.findall(pattern, html_content)
        return [f"https://www.armedicalboard.org/Public/verify/results.aspx?strPHIDNO={match}" 
                for match in matches]

    @staticmethod
    def parse_profile_information(html_content: str) -> Dict[str, str]:
        """Extract profile information from the HTML content."""
        fields = {
            'Name': r'Name:<span [^>]*>(.*?)<',
            'Primary Specialty': r'Primary Specialty:<span [^>]*>(.*?)<',
            'Mailing Address': r'Mailing Address:\s*<span [^>]*>(.*?)<',
            'City': r'City:<span [^>]*>(.*?)<',
            'State': r'State:\s*<span [^>]*>(.*?)<',
            #'Zip': r'Zip:<span [^>]*>(.*?)<',
            'Phone': r'Phone:<span [^>]*>(.*?)<',
            'License Number': r'License Number:\s*<span [^>]*>(.*?)<',
           # 'Original Issue Date': r'Original Issue Date:\s*<span [^>]*>(.*?)<',
            'Expiration Date': r'Expiration Date:<span [^>]*>(.*?)<',
            'License Status': r'License Status:\s*<span [^>]*>(.*?)<'
        }
        
        profile_info = {}
        for key, pattern in fields.items():
            match = re.search(pattern, html_content, re.DOTALL)
            profile_info[key] = match.group(1).strip() if match else 'Not available'
        
        return profile_info

    def process_profiles(self, last_name: str) -> int:
        """Process and store profiles for a given last name. Returns total valid profiles processed."""
        page = 1
        
        while True:
            logger.info(f"Fetching page {page} for last name: {last_name}")
            
            html_content = self.search_profiles_by_last_name(last_name, page)
            if not html_content:
                break
                
            profile_links = self.extract_profile_links(html_content)
            if not profile_links:
                logger.info(f"No more profiles found for {last_name} after page {page-1}")
                break
                
            for link in profile_links:
                try:
                    logger.info(f"Scraping profile: {link}")
                    profile_response = self.session.get(link, headers=self.headers)
                    profile_response.raise_for_status()
                    
                    profile_info = self.parse_profile_information(profile_response.text)
                    profile_info["Profile Link"] = link
                    
                    self.stats['total_scraped'] += 1
                    
                    # Insert profile immediately after scraping
                    if self.mongo_handler.insert_profile(profile_info):
                        self.stats['valid_profiles'] += 1
                    else:
                        self.stats['invalid_phone'] += 1
                    
                    sleep(self.request_delay)
                    
                except RequestException as e:
                    logger.error(f"Failed to scrape profile {link}: {str(e)}")
                    continue

            page += 1
            sleep(self.request_delay)
        
        return self.stats['valid_profiles']

    def scan_alphabet(self):
        """Scan through all letters of the alphabet."""
        for letter in string.ascii_uppercase:
            logger.info(f"Processing last names starting with: {letter}")
            try:
                profiles_count = self.process_profiles(letter)
                logger.info(f"Letter {letter} - Processed: {profiles_count} valid profiles")
                sleep(self.request_delay * 2)  # Extra delay between letters
            except Exception as e:
                logger.error(f"Error processing letter {letter}: {str(e)}")
        
        # Log final statistics
        logger.info("Scanning completed. Final statistics:")
        logger.info(f"Total profiles scraped: {self.stats['total_scraped']}")
        logger.info(f"Valid profiles stored: {self.stats['valid_profiles']}")
        logger.info(f"Profiles skipped (invalid phone): {self.stats['invalid_phone']}")

def main():
    try:
        scraper = MedicalBoardScraper()
        scraper.scan_alphabet()
        
    except KeyboardInterrupt:
        logger.info("Script execution interrupted by user")
        logger.info(f"Profiles processed before interruption: {scraper.stats}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()