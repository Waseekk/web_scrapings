import json
import logging
import random
import asyncio
import socket

from aiohttp import ClientSession, ClientError, ClientTimeout
from aiolimiter import AsyncLimiter 
from bs4 import BeautifulSoup
import pymongo
from pymongo.errors import DuplicateKeyError, PyMongoError

from zip_state_list import states, cities

# Initialize the rate limiter: 10 requests per 60 seconds 
limiter = AsyncLimiter(10, 60)

log_file = 'script.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file),
                        logging.StreamHandler()
                    ])


def load_config(file_path='config.json'):
    """Load configuration from a JSON file."""
    try:
        with open(file_path) as file:
            return json.load(file)
    except FileNotFoundError:
        logging.error(f"Config file '{file_path}' not found.")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from the config file '{file_path}'.")
        raise


async def fetch_profiles_page(session, api_url, params):
    """Fetch a single page of profiles from the API.

    Args:
        session (ClientSession): The aiohttp session to use for the request.
        api_url (str): The URL of the API endpoint.
        params (dict): The query parameters for the API request.

    Returns:
        list: A list of profile data dictionaries.
    """
    try:
        async with limiter: 
            async with session.get(api_url, params=params, timeout=ClientTimeout(total=50)) as response:
                response.raise_for_status()
                data = await response.json()

                if isinstance(data.get('data'), str) and "Unable to locate" in data.get('data'):
                    logging.warning(f"No data found for parameters {params}. Skipping.")
                    return []
                return data.get('data', {}).get('Items', [])

    except (asyncio.TimeoutError, ClientError) as error:
        logging.error(f"Error fetching data: {error}")
        return []
    except Exception as error:
        logging.error(f"Unexpected error: {error}")
        return []


async def extract_insurance_payment_and_specialties(
    session, 
    url, 
    include_address=False, 
    max_retries=3, 
    base_delay=2.5
):
    """
    Extract insurance/payment, specialties, and address information from a webpage.

    Args:
        session (aiohttp.ClientSession): The aiohttp session for making HTTP requests.
        url (str): The webpage URL to extract information from.
        include_address (bool): Whether to extract address information. Defaults to False.
        max_retries (int): Maximum number of retry attempts on timeout. Defaults to 3.
        base_delay (int or float): Base delay in seconds for exponential backoff. Defaults to 1.

    Returns:
        dict: A dictionary containing:
            - 'Insurance/Payment': List of extracted insurance/payment options.
            - 'Specialties': List of extracted specialties.
            - 'Address': List of extracted address components (if `include_address` is True).
            Returns None if max retries are exceeded.
    """
    attempts = 0
    while attempts < max_retries:
        try:
            async with limiter:
                async with session.get(url, timeout=ClientTimeout(total=50)) as response:
                    response.raise_for_status()
                    soup = BeautifulSoup(await response.text(), 'html.parser')

                    insurance_payment = extract_insurance_payment(soup)
                    specialties = extract_specialties(soup)
                    address = extract_address(soup) if include_address else []

                    return {
                        'Insurance/Payment': insurance_payment,
                        'Specialties': specialties,
                        'Address': address
                    }
        except asyncio.TimeoutError:
            attempts += 1
            logging.error(f"Timeout error when connecting to {url}. Retrying...")
        except socket.gaierror:
            logging.error(f"DNS resolution failed for {url}. Retrying...")
            break  # Exit retry loop for DNS errors
        except ClientError as error:
            logging.error(f"Client error: {error}")
            break
        except Exception as error:
            logging.error(f"Unexpected error: {error}")
            break

        delay = base_delay * (2 ** attempts) + random.uniform(0, 1)
        await asyncio.sleep(delay)

    return {'Insurance/Payment': [], 'Specialties': [], 'Address': []}


def extract_address(soup):
    """Extract address information from BeautifulSoup object as a list."""
    address = []
    address_section = soup.find('address')
    
    if address_section:
        p_tag = address_section.find('p')
        if p_tag:
            # Split the text into lines and strip any leading/trailing whitespace
            address = [line.strip() for line in p_tag.get_text(separator='\n').split('\n') if line.strip()]
    return address

def extract_insurance_payment(soup):
    """Extract insurance/payment information from BeautifulSoup object as a list."""
    insurance_payment_content = []
    experience_section = soup.find('div', class_='nutritionist-details__experience')
    
    if experience_section:
        insurance_heading = experience_section.find('h2', string=lambda text: text and 'Insurance/Payment' in text.strip())
        if insurance_heading:
            for sibling in insurance_heading.find_next_siblings():
                if sibling.name == 'h2':
                    break
                if sibling.name == 'p':
                    insurance_payment_content.append(sibling.get_text(strip=True))
    
    return insurance_payment_content

def extract_specialties(soup):
    """Extract specialties information from BeautifulSoup object as a list."""
    specialties = []
    experience_section = soup.find('div', class_='nutritionist-details__experience')
    
    if experience_section:
        specialties = [item.get_text(strip=True) for item in experience_section.find_all('p', class_='nutritionist-details__experience-item')]

    return specialties


async def connect_to_mongodb(uri, database_name, collection_name):
    """Connect to MongoDB and return the client and collection object.

    Args:
        uri (str): The MongoDB connection URI.
        database_name (str): The name of the MongoDB database.
        collection_name (str): The name of the MongoDB collection.

    Returns:
        tuple: A tuple containing the MongoDB client and collection object.
    """
    try:
        client = pymongo.MongoClient(uri, retryWrites=True, retryReads=True)
        db = client[database_name]
        collection = db[collection_name]
        collection.create_index([("Email", pymongo.ASCENDING)], unique=True)
        return client, collection
    
    except PyMongoError as error:
        logging.error(f"Failed to connect to MongoDB: {error}")
        return None, None


async def upsert_profiles_to_mongodb(collection, profiles_batch):
    """Upsert profiles data into MongoDB, skipping duplicates based on email."""
    try:
        for profile in profiles_batch:
            email = profile.get("Email", "")
            if not email:
                continue  
            
            existing_profile = collection.find_one({"Email": email})
            if existing_profile:
                logging.info(f"Profile with Email '{email}' already exists. Skipping.")
                continue

            result = collection.update_one(
                {"Email": email},
                {"$set": profile},
                upsert=True
            )
            
            if result.matched_count > 0:
                logging.info(f"Document with Email '{email}' updated.")
            elif result.upserted_id is not None:
                logging.info(f"New document with Email '{email}' inserted.")
        
    except DuplicateKeyError:
        logging.warning("Duplicate document detected.")
    except PyMongoError as error:
        logging.error(f"Failed to upsert documents: {error}")


def remove_empty_fields(d):
    """
    Recursively removes fields with empty or default values from a dictionary or list.

    - Removes 'Address' if it's a dictionary with all empty values.
    - For dictionaries, removes keys with None, empty strings, empty lists, or empty dictionaries.
    - For lists, removes items that are None, empty strings, empty lists, or empty dictionaries.

    Returns:
    dict or list: The cleaned data structure.
    """

    if isinstance(d, dict):
        if 'Address' in d:
            address = d['Address']
            if isinstance(address, dict) and not any(address.values()):
                d.pop('Address')
        
        return {k: remove_empty_fields(v) for k, v in d.items() if v not in [None, "", [], {}]}
    elif isinstance(d, list):
        return [remove_empty_fields(v) for v in d if v not in [None, "", [], {}]]
    else:
        return d

async def process_profiles(profiles, session, collection, upload_batch_size, include_address=False):
    """Process and upload profiles data to MongoDB.

    Args:
        profiles (list): A list of profiles data to process.
        session (ClientSession): The aiohttp session to use for making requests.
        collection (pymongo.collection.Collection): The MongoDB collection object.
        upload_batch_size (int): The batch size for uploading to MongoDB.
    """
    profiles_to_upload = []

    for profile in profiles:
        email = profile.get("Email", "")
        if not email:
            continue  

        existing_profile = collection.find_one({"Email": email})
        if existing_profile:
            logging.info(f"Profile with Email '{email}' already exists. Skipping.")
            continue

        url = f"https://www.eatright.org{profile.get('Url', '')}"
        insurance_payment_specialties_and_address = await extract_insurance_payment_and_specialties(session, url, include_address=include_address)

        if insurance_payment_specialties_and_address is None:
            logging.warning(f"Skipping profile for {email} due to failed extraction.")
            continue

        address = profile.get('Address', {})
        if address is None:
            address = {}

        phone = profile.get('Phone')
        if phone is None:
            phone = {}
        
        processed_profile = {
            "FullName": profile.get('FullName', ""),
            "Address": {
                "Name": address.get('Name', ""),
                "Line1": address.get('Line1', ""),
                "Line2": address.get('Line2', ""),
                "Line3": address.get('Line3', ""),
                "City": address.get('City', ""),
                "State": address.get('State', ""),
                "ZipCode": address.get('ZipCode', ""),
            },
            "Locations": profile.get('Locations', []),  
            "Phone": {
                "AreaCode": phone.get('AreaCode', ""),
                "Number": phone.get('Number', ""),
                "Extension": phone.get('Extension', ""),
            },
            "Email": email,
            "Website": profile.get('Website', ""),
            "Insurance/Payment": insurance_payment_specialties_and_address['Insurance/Payment'] or [],
            "Specialties": insurance_payment_specialties_and_address['Specialties'] or [],
        }

        if include_address:
            processed_profile["Address"] = insurance_payment_specialties_and_address['Address'] or []

        cleaned_profile = remove_empty_fields(processed_profile)
        profiles_to_upload.append(cleaned_profile)

        if len(profiles_to_upload) >= upload_batch_size:
            try:
                await upsert_profiles_to_mongodb(collection, profiles_to_upload)
            except Exception as e:
                logging.error(f"Failed to upload batch: {e}")
            profiles_to_upload = []

    if profiles_to_upload:
        await upsert_profiles_to_mongodb(collection, profiles_to_upload)


async def fetch_profiles_batch(api_url, params, batch_size, upload_batch_size, fetch_type='city', collection=None, session=None):
    """Fetch and process profiles data in batches.

    Args:
        api_url (str): The API URL to fetch data from.
        params (dict): The query parameters for the API request.
        batch_size (int): The number of profiles to fetch per batch.
        upload_batch_size (int): The batch size for uploading to MongoDB.
        fetch_type (str): The type of location to fetch data for ('city' or 'state').
        collection (pymongo.collection.Collection): The MongoDB collection object.
        session (ClientSession): The aiohttp session to use for making requests.
    """
    locations = cities if fetch_type == 'city' else states
    include_address = fetch_type == 'state'  
    batch_number = 1  

    for location in locations:
        params[fetch_type] = location
        current_page = 1

        while True:
            params['page'] = current_page
            params['perPage'] = batch_size
            params['type'] = 'in-person' if fetch_type == 'city' else 'telehealth'

            logging.info(f"Batch {batch_number}: Fetching page {current_page} for {fetch_type} {location} with {batch_size} profiles...")
            profiles = await fetch_profiles_page(session, api_url, params)

            if not profiles:
                logging.info(f"Batch {batch_number}: No more profiles found for {fetch_type} {location}. Moving to next location.")
                break

            logging.info(f"Batch {batch_number}: Fetched {len(profiles)} profiles on page {current_page}. Processing each profile...")

            await process_profiles(profiles, session, collection, upload_batch_size, include_address=include_address)

            if len(profiles) < batch_size:
                break

            current_page += 1
            batch_number += 1  

            await asyncio.sleep(random.uniform(1, 5))

        await asyncio.sleep(random.uniform(1, 5))


async def main():
    """Main entry point for the script."""
    config = load_config()
    batch_size = config["batch_size"]
    upload_batch_size = config["upload_batch_size"]
    api_url = config["api_url"]
    zip_info = config.get("zip_info", {})
    zip_list= zip_info.get("zip_list_number", 0)

    # Validate zip_list value
    if not (1 <= zip_list <= 199):
        logging.error(f"Invalid zip_list value: {zip_list}. It must be between 1 and 199. Please change the value in config.json")
        return

    fetch_type = input("Enter fetch type ('city' or 'state'): ").strip().lower()

    if fetch_type not in ['city', 'state']:
        print("Invalid fetch type. Please enter 'city' or 'state'.")
        return

    client, collection = await connect_to_mongodb(
        uri=config["mongodb_uri"],
        database_name=config["database_name"],
        collection_name=config["collection_name"]
    )

    if not collection:
        logging.error("MongoDB connection failed. Exiting the script.")
        return

    async with ClientSession() as session:
        await fetch_profiles_batch(api_url, {}, batch_size, upload_batch_size, fetch_type=fetch_type, collection=collection, session=session)

    if client:
        client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError:
        # Handle the case where asyncio.run is called within a running event loop
        logging.error("Asyncio event loop already running, use 'await main()' directly in interactive environments.")
    

