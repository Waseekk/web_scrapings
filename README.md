# Nutrition Experts Data Fetcher
This project is a Python script designed to fetch and process data about nutrition experts from the EatRight.org API. The script extracts relevant information from expert profiles, such as insurance/payment details and specialties, and uploads the processed data into a MongoDB database. It handles pagination, processes data in batches, and logs progress and errors.

# Install required libraries
You can install the required libraries using pip:
`pip install -r requirements.txt`

# Api
The script fetches data from the API endpoint: https://www.eatright.org/api/find-a-nutrition-expert.

# Update the Database and Collection name if Necessary
Modify the database_name and collection_name variables to specify your database and collection from config.json

database_name = "your_database_name",
collection_name = "your_collection_name",
You can also select the zip_list_number to fetch  nutrition profiles from a certain range of zip code lists.

# Run the Scripts
Run `python zip_state_list.py` to get the zip code and state lists.
Run 'python nutritionist_scraper.py' and select input of 'city' or 'state' for fetching the profiles.