
import pandas as pd
import random
import requests
from bs4 import BeautifulSoup
import time
from pymongo import MongoClient
import logging


#The list of the dentist profiles is collected by querying by the alphabets (A*, B*). website: 'https://azbodv7prod.glsuite.us/GLSuiteWeb/clients/azbod/public/WebVerificationSearch.aspx'

# import requests
# from bs4 import BeautifulSoup
# import pandas as pd

# # Target URL
# url = "https://azbodv7prod.glsuite.us/GLSuiteWeb/Clients/AZBOD/public/WebVerificationSearchResultsPRO.aspx"

# # Headers
# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
#     "Referer": "https://azbodv7prod.glsuite.us/GLSuiteWeb/clients/azbod/public/WebVerificationSearch.aspx",
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
#     "Accept-Encoding": "gzip, deflate, br, zstd",
#     "Accept-Language": "en-US,en;q=0.9",
#     "Connection": "keep-alive"
# }

# # Cookies
# cookies = {
#     "ASP.NET_SessionId": "2ig404aomegllnmhivdrgxzo",
#     "ApplicationGatewayAffinity": "2279ae3f0487f000d1cefab7e8ce8c97",
#     "ApplicationGatewayAffinityCORS": "2279ae3f0487f000d1cefab7e8ce8c97"
# }

# # Make the GET request
# response = requests.get(url, headers=headers, cookies=cookies)

# # Check response
# if response.status_code == 200:
#     print("Request successful!")
    
#     # Parse the response text
#     soup = BeautifulSoup(response.text, "html.parser")

#     # Find all <a> tags inside the results table
#     name_link_list = []
#     table = soup.find("table", {"id": "ContentPlaceHolder1_dtgGeneral"})  # Locate the table
#     if table:
#         for row in table.find_all("tr")[1:]:  # Skip the header row
#             link_tag = row.find("a")
#             if link_tag and "href" in link_tag.attrs:
#                 name = link_tag.get_text(strip=True)
#                 link = link_tag["href"]
#                 name_link_list.append({"Name": name, "Profile Link": link})

#     # Create a DataFrame from the list
#     df_Z = pd.DataFrame(name_link_list)

#     # Display the DataFrame
#     print(df_Z)


# else:
#     print(f"Request failed with status code: {response.status_code}")


df_all_profiles=pd.read_csv('all_profile_link.csv')


# Base URL and headers
base_url = "https://azbodv7prod.glsuite.us/GLSuiteWeb/Clients/AZBOD/public/"
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1",
]

cookies = {
    "ASP.NET_SessionId": "2ig404aomegllnmhivdrgxzo",
    "ApplicationGatewayAffinity": "2279ae3f0487f000d1cefab7e8ce8c97",
    "ApplicationGatewayAffinityCORS": "2279ae3f0487f000d1cefab7e8ce8c97"
}


# Configure logging
logging.basicConfig(filename="arizona_script.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

client = MongoClient("mongodb://127.0.0.1:27017/")

# Select the appropriate database and collection
db = client["eat-right_counselor_marketing"]
collection = db["dentist_profiles"] 

# Ensure unique index on "Phone Number"
collection.create_index("Phone Number", unique=True)

profile_data_list = []

# Correct the URLs in the DataFrame by appending the base URL
df_all_profiles["Profile Link"] = df_all_profiles["Profile Link"].apply(lambda link: base_url + link if not link.startswith("https://") else link)

# Process each corrected URL
for _, row in df_all_profiles.iterrows():
    profile_link = row["Profile Link"]
    name = row["Name"]

    # Randomly select a User-Agent
    headers = {
        "User-Agent": random.choice(user_agents),
        "Referer": base_url,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }

    logging.info(f"Processing: {name} - {profile_link}")

    try:
        # Fetch profile page
        response = requests.get(profile_link, headers=headers, cookies=cookies)

        if response.status_code == 200:
            # Parse the profile page
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract General Information
            general_info = []
            general_table = soup.find("table", {"id": "ContentPlaceHolder1_dtgGeneralN"})
            if general_table:
                for row in general_table.find_all("tr"):
                    info = row.get_text(strip=True)
                    general_info.append(info)

            general_details = {
                "Name": general_info[0] if len(general_info) > 0 else None,
                "Address": ", ".join(general_info[1:3]) if len(general_info) > 2 else None,
                "Phone Number": general_info[3] if len(general_info) > 3 else None
            }

            # Skip if no phone number
            if not general_details["Phone Number"]:
                logging.info(f"Skipping {name} due to missing phone number.")
                continue

            # Extract License Information
            license_info = {}
            license_table = soup.find("table", {"id": "ContentPlaceHolder1_dtgGeneral"})
            if license_table:
                for row in license_table.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) == 2:
                        key = cells[0].get_text(strip=True).replace("License Number", "").strip()
                        value = cells[1].get_text(strip=True)
                        if key and key != ":":  # Skip any empty or malformed keys
                            license_info[key] = value

            # Extract Certifications
            certifications = []
            certification_name = soup.find("input", {"id": "ContentPlaceHolder1_tbNameCert1"})
            if certification_name:
                certification = {"Certification Name": certification_name.get("value", "").strip()}
                certification_table = soup.find("table", {"id": "ContentPlaceHolder1_dtgCert1"})
                if certification_table:
                    for row in certification_table.find_all("tr"):
                        cells = row.find_all("td")
                        if len(cells) == 2:
                            key = cells[0].get_text(strip=True).replace(":", "")
                            value = cells[1].get_text(strip=True)
                            certification[key] = value
                certifications.append(certification)

            # Flatten Certifications into separate columns
            certification_columns = {}
            if certifications:
                for i, cert in enumerate(certifications, start=1):
                    for key, value in cert.items():
                        certification_columns[f"Certification {i} - {key}"] = value

            # Combine all extracted data
            profile_data = {
                "Name": general_details["Name"],
                "Address": general_details["Address"],
                "Phone Number": general_details["Phone Number"],
                **license_info,  # Add license info into separate columns (excluding License Number)
                **certification_columns  # Add certifications as separate columns
            }

            # Insert the profile data into MongoDB
            try:
                collection.insert_one(profile_data)
            except Exception as e:
                logging.warning(f"Skipping duplicate phone number for {name}: {e}")

        else:
            logging.warning(f"Failed to fetch profile page for {name} with status code: {response.status_code}")

    except Exception as e:
        logging.error(f"Error processing {name}: {e}")

    # Add a random delay between 1 and 3 seconds
    time.sleep(random.uniform(1, 3))

logging.info("Data insertion into MongoDB completed.")

