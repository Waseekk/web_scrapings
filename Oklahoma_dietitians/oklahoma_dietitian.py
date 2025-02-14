import requests
from bs4 import BeautifulSoup
import logging
import time
import random
from pymongo import MongoClient, errors

# Configure logging
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        filename="scraping.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

def fetch_dietitian_data(mongo_uri="mongodb://127.0.0.1:27017/", database_name="profession_lead", collection_name="Oklahama_dietitians"):
    logging.info("Script started.")

    # MongoDB setup
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Create a unique index for "Phone #:"
    collection.create_index("Phone #:", unique=True)

    url = "https://www.okmedicalboard.org/dietitians/search"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Referer": url,
    }

    base_payload = {
        "licensenbr": "",
        "lictype": "LD",
        "lname": "",
        "fname": "",
        "practcounty": "55",
        "status": "ACTIVE",
        "discipline": "",
        "licensedat_range": "",
        "order": "lname",
        "show_details": "1",
        "current_page": "1",
    }

    while True:
        try:
            logging.info(f"Sending POST request for page {base_payload['current_page']}...")
            response = requests.post(url, headers=headers, data=base_payload, timeout=10)

            if response.status_code == 200:
                logging.info(f"Successfully fetched data from page {base_payload['current_page']}.")
                soup = BeautifulSoup(response.text, 'html.parser')

                tables = soup.find_all('table', class_='licensee-info')
                if not tables:
                    logging.info("No more results found. Exiting pagination loop.")
                    break

                for table in tables:
                    # Extract and clean the name
                    name_header = table.find('th')
                    if name_header:
                        name = name_header.get_text(strip=True).replace("\xa0", " ").split("Printer-Friendly Version")[0].strip()
                    else:
                        name = "N/A"

                    rows = table.find_all('tr')
                    license_data = {"Name": name}
                    for row in rows:
                        columns = row.find_all('th')
                        values = row.find_all('td')
                        for col, val in zip(columns, values):
                            field = col.text.strip()
                            value = val.text.strip() if val else "N/A"
                            license_data[field] = value

                    # Improved phone number check
                    phone_field = license_data.get("Phone #:", "").strip()
                    if phone_field and phone_field != "N/A":
                        sanitized_data = {key.replace('.', '_'): value for key, value in license_data.items()}
                        try:
                            # Check for existing document with the same phone number
                            existing_doc = collection.find_one({"Phone #:": phone_field})
                            if existing_doc:
                                # Update the existing document
                                collection.update_one({"Phone #:": phone_field}, {"$set": sanitized_data})
                                logging.info(f"Updated profile in MongoDB: {sanitized_data}")
                            else:
                                # Insert a new document
                                collection.insert_one(sanitized_data)
                                logging.info(f"Inserted profile into MongoDB: {sanitized_data}")
                        except errors.DuplicateKeyError:
                            logging.warning(f"Duplicate phone number found. Skipping profile: {phone_field}")
                    else:
                        logging.info(f"Skipped profile (no phone number): {license_data}")

                base_payload["current_page"] = str(int(base_payload["current_page"]) + 1)
                delay = random.uniform(2, 5)
                logging.info(f"Sleeping for {delay:.2f} seconds before the next request.")
                time.sleep(delay)
            else:
                logging.error(f"Request failed on page {base_payload['current_page']} with status code {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error on page {base_payload['current_page']}: {e}")
            break
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            break

    logging.info("Script finished.")

if __name__ == "__main__":
    fetch_dietitian_data()
