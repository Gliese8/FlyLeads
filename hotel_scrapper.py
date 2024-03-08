import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime, timedelta
import time


class _ScrapeHotel:
    def __init__(self, destination):
        self._destination = destination
        self._date_check_in = self._calculate_closest_friday(datetime.today() + timedelta(days=60))
        self._date_check_out = self._date_check_in + timedelta(days=2)

    def _calculate_closest_friday(self, future_date):
        # Calculate the weekday number (0: Monday, 1: Tuesday, ..., 6: Sunday)
        weekday_number = future_date.weekday()

        # Calculate the number of days to add to reach the closest Friday
        days_to_add = (4 - weekday_number) % 7

        # Calculate the closest Friday by adding the remaining days
        closest_friday = future_date + timedelta(days=days_to_add)

        return closest_friday.date()

    def _make_url(self):
        return (f'https://www.google.com/travel/search?q={self._destination}&'
                f'checkin={self._date_check_in.strftime("%Y-%m-%d")}&'
                f'checkout={self._date_check_out.strftime("%Y-%m-%d")}')

    def _accept_cookies(self, driver):
        try:
            wait = WebDriverWait(driver, 10)
            accept_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Accept all')]")))
            accept_button.click()
            print("Accepted cookies.")
        except TimeoutException:
            print("Accept button not found or not clickable.")

    def _click_all_filters(self, driver):
        try:
            all_filters_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'All filters')]"))
            )
            all_filters_button.click()
            print("Clicked on 'All filters'.")
        except TimeoutException:
            print("Failed to find 'All filters' button within the given time.")

    def _click_lowest_price(self, driver):
        try:
            lowest_price_label = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "//label[@class='VfPpkd-V67aGc' and text()='Lowest price']"))
            )
            lowest_price_label.click()
            print("Clicked on 'Lowest price' label.")
            time.sleep(5)  # Add a 5-second wait after clicking on 'Lowest price'
        except TimeoutException:
            print("Failed to find the 'Lowest price' label within the given time.")

    def scrape(self):
        options = webdriver.ChromeOptions()
        options.headless = True  # Set False to watch the browser actions
        with webdriver.Chrome(options=options) as driver:
            driver.get(self._make_url())

            self._accept_cookies(driver)
            self._click_all_filters(driver)
            self._click_lowest_price(driver)

            try:
                # Find the hotel name and its corresponding lowest price
                hotel_element = driver.find_element(By.XPATH, "//a[@class='PVOOXe']")
                price_element = driver.find_element(By.XPATH,
                                                    "//span[@jsaction='mouseenter:JttVIc;mouseleave:VqIRre;']")
                hotel_name = hotel_element.get_attribute('aria-label')
                lowest_price_str = price_element.text

                # Convert the lowest price to float, multiply by 2, and convert back to string
                lowest_price = float(lowest_price_str.replace('€', '').
                                     replace(',', '').strip())
                lowest_price *= 2

                return hotel_name, lowest_price

            except TimeoutException:
                print("Failed to find the hotel information within the given time.")

