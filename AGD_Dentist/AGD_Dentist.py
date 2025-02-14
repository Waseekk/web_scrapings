import logging
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Setup ChromeDriver with options
driver_path = 'chromedriver-win64//chromedriver-win64//chromedriver.exe'
service = Service(driver_path)

options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")  # Avoid detection as an automated browser
options.add_argument("--start-maximized")  # Start browser in maximized mode
options.add_argument("--disable-extensions")  # Disable unnecessary extensions
# Uncomment the following line to run in headless mode
# options.add_argument("--headless")

# Create a ChromeDriver instance
driver = webdriver.Chrome(service=service, options=options)

try:
    # Navigate to the website
    driver.get('https://www.agd.org/practice/tools/patient-resources/find-an-agd-dentist')
    logging.info(f"Page title: {driver.title}")

    # Fill out the search form
    # Wait for the address input field to appear and enter the location
    address_field = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "searchText"))
    )
    address_field.send_keys("New York")  # Example ZIP/Postal Code or Address
    
    # Select a distance from the dropdown menu using JavaScript
    distance_dropdown = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "searchMileDropdown"))
    )
    driver.execute_script("arguments[0].value = '50';", distance_dropdown)

    # Locate and click the submit button
    submit_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//input[@type='submit' and @value='FIND AN AGD DENTIST']"))
    )
    submit_button.click()

    # Pause execution to allow manual reCAPTCHA solving
    input("Solve the reCAPTCHA manually, then press Enter to continue...")

    # Initialize a list to store dentist details and a counter for numbering
    dentists = []
    counter = 1

    # Pagination loop to extract dentist details from all pages
    while True:
        try:
            # Wait for the results section to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "sfPublicWrapper"))
            )

            # Find dentist names and their corresponding links on the current page
            dentist_names = driver.find_elements(By.XPATH, "//h3[@class='title']")
            dentist_links = driver.find_elements(By.XPATH, "//a[contains(@data-dentist-title, '')]")

            # Iterate through the names and links, storing the details with a sequential number
            for name, link in zip(dentist_names, dentist_links):
                dentist_name = name.text
                dentist_link = link.get_attribute("href")
                dentists.append({"number": counter, "name": dentist_name, "link": dentist_link})
                logging.info(f"Name {counter}: {dentist_name}, Link: {dentist_link}")
                counter += 1

            # Check if a "Next" button is available for pagination
            next_button = driver.find_elements(By.XPATH, "//li[@class='PagedList-skipToNext']/a[@rel='next']")
            if next_button:
                next_button[0].click()  # Click the "Next" button
                time.sleep(2)  # Wait for the next page to load
            else:
                logging.info("No more pages. Exiting pagination.")
                break  # Exit the loop if no "Next" button is found

        except Exception as e:
            # Log any errors during pagination and break the loop
            logging.error(f"Error during pagination: {e}")
            break

    # Save extracted dentist details to a CSV file
    with open("dentist_links_sequential.csv", "w", newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["Number", "Name", "Link"])
        writer.writeheader()  # Write the CSV header
        for dentist in dentists:
            writer.writerow({"Number": dentist['number'], "Name": dentist['name'], "Link": dentist['link']})

    logging.info(f"Scraped {len(dentists)} dentists. Results saved to 'dentist_links_sequential.csv'.")

except Exception as e:
    # Log any errors encountered during execution
    logging.error(f"Error encountered: {e}")
finally:
    # Ensure the browser is closed in case of success or failure
    driver.quit()
