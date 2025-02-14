import json

# state list available on eat-right
states = [
    "Alabama",
     "Alaska",
    "American Samoa",
    "Arizona",
    "Arkansas",
    "Armed Forces Africa, Canada, Europe and Middle East",
    "Armed Forces Americas (except Canada)",
    "Armed Forces Pacific",
     "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Diplomatic Pouch Services",
    "District of Columbia",
    "Fleet Post Office",
    "Florida",
    "Georgia",
    "Guam",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Northern Mariana Islands",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Puerto Rico",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virgin Islands",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming"
]


# Load configuration from config.json
def load_config():
    with open('config.json', 'r') as file:
        config = json.load(file)
    return config

# Function to generate ZIP code lists
def generate_zip_code_lists(start, end, chunk_size, excluded_ranges):
    def generate_zip_codes(start, end):
        return [f"{zip_code:05d}" for zip_code in range(start, end + 1)]

    zip_code_lists = []
    current_start = start
    list_number = 1

    while current_start <= end:
        current_end = min(current_start + chunk_size - 1, end)

        if any(excl_start <= current_end and excl_end >= current_start for excl_start, excl_end in excluded_ranges):
            current_start = current_end + 1
            continue

        zip_code_list = generate_zip_codes(current_start, current_end)
        list_name = f"zip_{list_number}"
        zip_code_lists.append((list_name, zip_code_list))
        current_start = current_end + 1
        list_number += 1

    return zip_code_lists

# Load configuration and extract parameters
config = load_config()
zip_info = config.get("zip_info", {})
start_zip = zip_info.get("start_zip", 501)
end_zip = zip_info.get("end_zip", 99950)
chunk_size = zip_info.get("chunk_size", 500)
excluded_ranges = zip_info.get("excluded_ranges", [])
zip_list_number = zip_info.get("zip_list_number", 1)  # Default to 1 if zip_list_number is not found

# Generate the ZIP code lists
zip_code_lists = generate_zip_code_lists(start_zip, end_zip, chunk_size, excluded_ranges)

# Function to access a specific ZIP code list by name
def get_zip_code_list_by_name(name):
    for list_name, zip_code_list in zip_code_lists:
        if list_name == name:
            return zip_code_list
    return None

# Retrieve the ZIP list number and get the corresponding list
specific_list_name = f"zip_{zip_list_number}"
cities = get_zip_code_list_by_name(specific_list_name)
