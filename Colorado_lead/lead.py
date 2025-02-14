import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from pymongo import MongoClient

# Setup logging
logging.basicConfig(
    filename='script.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MongoDB connection
mongo_client = MongoClient('mongodb://127.0.0.1:27017/')  # Local MongoDB connection
db = mongo_client['eat-right_counselor_marketing']  # Replace with  database name
collection = db['colorado_lead']  # ew collection name

# Ensure phone numbers are unique by creating an index
collection.create_index('Phone Number', unique=True)

# Load the dataset
#file_path = 'Professional_and_Occupational_Licenses_in_Colorado.csv'
#df = pd.read_csv(file_path)
df = pd.read_parquet('Active_Licenses_Links.parquet', engine='pyarrow')
# Filter active licenses with non-NaN URLs
active_df = df[df['linkToViewHealthcareProfile'].notna()]

# Function to fetch data from a URL
def fetch_data(row, row_index):
    url = row['linkToViewHealthcareProfile']
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract Name
        name_cell = soup.find('td', string='Name')
        name = name_cell.find_next_sibling('td').text.strip() if name_cell else "Not Found"

        # Extract Practice Locations
        practice_table = soup.find('table', border="1")
        practice_locations = []
        if practice_table:
            rows = practice_table.find_all('tr')[1:]  # Skip header row
            for row in rows:
                cols = [col.text.strip() for col in row.find_all('td')]
                if len(cols) == 5 and cols[4]:  # Ensure "Phone Number" is present
                    practice_locations.append({
                        "Address": cols[0],
                        "City": cols[1],
                        "State": cols[2],
                        "Zip Code": cols[3],
                        "Phone Number": cols[4]
                    })

        # Skip rows if no valid phone number is found
        if not practice_locations:
            logging.warning(f"Row {row_index} (Name: {name}): No phone number found, skipping.")
            return None

        # Return all locations for this row
        results = []
        for location in practice_locations:
            results.append({
                "Row": row_index,
                "Name": name,
                **location
            })
        return results

    except Exception as e:
        logging.error(f"Row {row_index}: Error occurred - {e}")
        return None

# Process each row in the dataset
for index, row in active_df.iterrows():
    data = fetch_data(row, index)

    if data:  # Only insert rows with valid phone numbers
        for item in data:
            try:
                collection.insert_one(item)  # Insert each location as a separate document in MongoDB
                logging.info(f"Row {index} (Name: {item['Name']}): Data inserted into MongoDB.")
            except Exception as e:
                logging.warning(f"Row {index} (Phone: {item['Phone Number']}): Duplicate phone number, skipping.")
    else:
        logging.warning(f"Row {index}: Skipped due to missing phone number.")

    # Add random sleep time between requests
    time.sleep(random.uniform(1, 2))  # Sleep for 1-2 seconds

# Final log message
logging.info("All rows processed. Data inserted into MongoDB.")
print("All rows processed. Check 'script_2.log' for details.")
