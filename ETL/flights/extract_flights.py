import os
import time

from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.support.ui import Select

start_time = time.time()

# Create the directory to store the data
download_dir = os.path.abspath("../../data/flights/raw")
os.makedirs(download_dir, exist_ok=True)
# Set the download directory in the Chrome options
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory": download_dir}
chrome_options.add_experimental_option("prefs", prefs)

url = "https://transtats.bts.gov/ONTIME/Arrivals.aspx"
driver = webdriver.Chrome(options=chrome_options)
driver.get(url)

driver.find_element(value="chkStatistics_0").click()  # Scheduled Arrival Time
driver.find_element(value="chkStatistics_1").click()  # Actual Arrival Time
driver.find_element(value="chkStatistics_4").click()  # Arrival Delay
driver.find_element(value="chkStatistics_7").click()  # Causa of Delay

driver.find_element(value="chkAllMonths").click()  # Select all months
driver.find_element(value="chkAllDays").click()  # Select all days
driver.find_element(value="chkYears_18").click()  # Select 2005
driver.find_element(value="chkYears_19").click()  # Select 2006

airports = Select(driver.find_element(value="cboAirport"))
airlines = Select(driver.find_element(value="cboAirline"))

for i in range(len(airports.options)):
    # Refresh the airport dropdown and select the next option
    airports = Select(driver.find_element(value="cboAirport"))
    airport_option = airports.options[i]
    airport_txt = airport_option.text
    airports.select_by_visible_text(airport_txt)

    airlines = Select(driver.find_element(value="cboAirline"))
    for j in range(len(airlines.options)):
        # Refresh the airline dropdown and select the next option
        airlines = Select(driver.find_element(value="cboAirline"))
        airline_option = airlines.options[j]
        airline_txt = airline_option.text
        airlines.select_by_visible_text(airline_txt)

        # Click the submit button
        driver.find_element(value="btnSubmit").click()

        # Try to download the data
        try:
            download_button = driver.find_element(value="DL_CSV")
            print(f"Downloading data for: {airport_txt} - {airline_txt}")
            download_button.click()
        except NoSuchElementException:
            print(f"No data for: {airport_txt} - {airline_txt}")

end_time = time.time()
print("Time taken: ", end_time - start_time, "seconds")
