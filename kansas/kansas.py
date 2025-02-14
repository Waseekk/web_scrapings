import requests
from bs4 import BeautifulSoup
import time
import logging
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from requests.adapters import HTTPAdapter
import random
from urllib3.util.retry import Retry
from typing import Optional, Dict, List
import json

# Set up logging
logging.basicConfig(
    filename=f'scraper_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RequestError(Exception):
    """Custom exception for request-related errors"""
    pass

class MongoDBHandler:
    def __init__(self, connection_string: str, database: str, collection: str):
        self.client = MongoClient(connection_string)
        self.db = self.client[database]
        self.collection = self.db[collection]
        
        # Create unique index on phone number since we're only storing records with phones
        self.collection.create_index([("Phone", ASCENDING)], unique=True)
    
    def insert_many(self, documents: List[Dict]):
        try:
            # Prepare documents with proper handling of empty values
            processed_docs = []
            for doc in documents:
                # Clean empty strings to None for better MongoDB handling
                cleaned_doc = {
                    k: (None if v == '' else v) 
                    for k, v in doc.items()
                }
                processed_docs.append(cleaned_doc)

            # Use ordered=False to continue inserting even if some documents fail
            result = self.collection.insert_many(processed_docs, ordered=False)
            return len(result.inserted_ids)
        except Exception as e:
            if "duplicate key error" in str(e):
                # Extract number of successful inserts from bulk write error
                if hasattr(e, 'details'):
                    return e.details.get('nInserted', 0)
                logging.warning("Duplicate records found and skipped")
                return 0
            logging.error(f"Error inserting documents: {str(e)}")
            return 0
    
    def close(self):
        self.client.close()

def create_session() -> requests.Session:
    """Create a session with retry logic and proper headers"""
    session = requests.Session()
    
    # Set up retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set headers based on your actual request headers
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows"
    })
    
    return session

def validate_response(response: requests.Response, context: str):
    """Validate response and handle common errors"""
    if response.status_code == 429:
        raise RequestError(f"Rate limit exceeded during {context}")
    elif response.status_code == 403:
        raise RequestError(f"Access forbidden during {context}")
    elif response.status_code != 200:
        raise RequestError(f"Unexpected status code {response.status_code} during {context}")
    
    if not response.text:
        raise RequestError(f"Empty response received during {context}")

def get_page_results(soup: BeautifulSoup) -> List[Dict]:
    """Extract results from a single page with additional error checking"""
    results = []
    tbody = soup.find('tbody')
    
    if not tbody:
        logging.warning("No tbody found in page results")
        return []
    
    rows = tbody.find_all('tr')
    
    for row in rows:
        try:
            cells = row.find_all('td')
            if len(cells) < 5:
                logging.warning(f"Row has insufficient cells: {len(cells)}")
                continue
                
            profile_link = cells[0].find('a')
            if not profile_link or not profile_link.get('href'):
                logging.warning("Missing profile link")
                continue
                
            results.append({
                'name': cells[0].text.strip(),
                'profession': cells[1].text.strip(),
                'license_number': cells[2].text.strip(),
                'location': cells[3].text.strip(),
                'status': cells[4].text.strip(),
                'profile_link': f"https://www.kansas.gov{profile_link['href']}"
            })
        except Exception as e:
            logging.error(f"Error processing row: {str(e)}")
            continue
            
    return results

def get_profile_details(session: requests.Session, url: str) -> Optional[Dict]:
    """Extract detailed information from a profile page with enhanced error handling"""
    try:
        response = session.get(url)
        validate_response(response, "profile details")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Helper function to safely extract text
        def get_field_text(strong_text: str) -> Optional[str]:
            element = soup.find('strong', text=strong_text)
            if not element:
                logging.warning(f"Field not found: {strong_text}")
                return None
            return element.next_sibling.strip() if element.next_sibling else None
        
        # Required fields
        name = soup.find('h3')
        if not name:
            raise ValueError("Name field not found")
        
        details = {
            'Name': name.text.replace('Profile for ', '').strip(),
            'Profession': get_field_text('Profession:'),
            'Address': None,
            'Phone': get_field_text('Phone:'),
          #  'Fax': get_field_text('Fax:'),
          #  'Year of Birth': get_field_text('Year of Birth:'),
          #  'School Name': get_field_text('School Name:'),
          #  'Degree Date': get_field_text('Degree Date:'),
           # 'License Number': get_field_text('License Number:'),
            'License Type': get_field_text('License Type:'),
            'License Status': get_field_text('License Status:'),
            'License Expiration Date': get_field_text('License Expiration Date:'),
          #  'Original License Date': get_field_text('Original License Date:'),
            'Last Renewal Date': get_field_text('Last Renewal Date:')
        }
        
        # Special handling for address
        address_strong = soup.find('strong', text='Address:')
        if address_strong and address_strong.find_next('br'):
            details['Address'] = address_strong.find_next('br').next_sibling.strip()
        
        return details
        
    except Exception as e:
        logging.error(f"Error getting profile details for {url}: {str(e)}")
        return None

def scrape_profession(session: requests.Session, profession_code: str, mongodb_handler: MongoDBHandler) -> int:
    """Scrape all data for a single profession and store in MongoDB"""
    url = "https://www.kansas.gov/ssrv-ksbhada/search.html"
    data = {'profession': profession_code}
    records_processed = 0
    
    try:
        response = session.post(url, data=data)
        validate_response(response, "profession search")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        page_number = 1
        
        while True:
            logging.info(f"Processing profession {profession_code} - page {page_number}")
            
            page_results = get_page_results(soup)
            if not page_results:
                break
            
            batch = []
            for result in page_results:
                profile_details = get_profile_details(session, result['profile_link'])
                if profile_details:
                    result.update(profile_details)
                    batch.append(result)
                
                #  delay between profile requests
                time.sleep(random.uniform(.5, 1.5)) 
            
            # Filter out profiles without phone numbers and insert into MongoDB
            valid_batch = [record for record in batch if record.get('Phone') and record['Phone'].strip()]
            if valid_batch:
                inserted_count = mongodb_handler.insert_many(valid_batch)
                records_processed += inserted_count
                logging.info(f"Inserted {inserted_count} records with phone numbers for profession {profession_code}")
            skipped_count = len(batch) - len(valid_batch)
            if skipped_count > 0:
                logging.info(f"Skipped {skipped_count} records without phone numbers")
            
            # Handle pagination
            pagination = soup.find('div', class_='pagination')
            next_link = pagination.find('a', text='Next') if pagination else None
            
            if not next_link:
                break
            
            response = session.get("https://www.kansas.gov/ssrv-ksbhada/results.html?navigate=next")
            validate_response(response, "pagination")
            soup = BeautifulSoup(response.text, 'html.parser')
            page_number += 1
            
            # Consistent delay between pages
            time.sleep(random.uniform(1.5, 3.5))
            
    except RequestError as e:
        logging.error(f"Request error for profession {profession_code}: {str(e)}")
    except Exception as e:
        logging.error(f"Error processing profession {profession_code}: {str(e)}")
    
    return records_processed

def main():
    # MongoDB configuration
    MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
    DATABASE_NAME = "professional_lead"
    COLLECTION_NAME = "kansas_board_of_healing_arts"
    
    # List of profession codes to scrape
    profession_codes = [
        '23',  # Licensed Acupuncturist
        '24',  # Athletic Trainer
        '01', #Chiropractor
        '75','04','21','21A','17','18','11','14','15','12','94','22','16','19','08'
        
    ]
    
    total_records = 0
    
    try:
        mongodb_handler = MongoDBHandler(MONGO_CONNECTION_STRING, DATABASE_NAME, COLLECTION_NAME)
        session = create_session()
        
        for profession_code in profession_codes:
            try:
                records = scrape_profession(session, profession_code, mongodb_handler)
                total_records += records
                
                logging.info(f"Completed profession {profession_code} - {records} records processed")
                print(f"Completed profession {profession_code} - {records} records processed")
                
                # Consistent delay between professions
                time.sleep(random.uniform(3, 4))
                
            except Exception as e:
                logging.error(f"Failed to process profession {profession_code}: {str(e)}")
                continue
        
        logging.info(f"Scraping completed. Total records collected: {total_records}")
        print(f"Scraping completed. Total records collected: {total_records}")
        
    except Exception as e:
        logging.error(f"Critical error in main execution: {str(e)}")
    finally:
        mongodb_handler.close()

if __name__ == "__main__":
    main()