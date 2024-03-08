from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime, timedelta
import re


class _ScrapeFlight:
    def __init__(self, origin, dest):
        self._origin = origin
        self._dest = dest
        self._date_leave = self._calculate_closest_friday(datetime.today() + timedelta(days=60))
        self._date_return = self._date_leave + timedelta(days=2)

    def _calculate_closest_friday(self, future_date):
        # Calculate the weekday number (0: Monday, 1: Tuesday, ..., 6: Sunday)
        weekday_number = future_date.weekday()

        # Calculate the number of days to add to reach the closest Friday
        days_to_add = (4 - weekday_number) % 7

        # Calculate the closest Friday by adding the remaining days
        closest_friday = future_date + timedelta(days=days_to_add)

        return closest_friday.date()

    def _make_url(self):
        return (f'https://www.google.com/travel/flights?q=Flights%20to%20{self._dest}%'
                f'20from%20{self._origin}%20on%20{self._date_leave.strftime("%Y-%m-%d")}%'
                f'20through%20{self._date_return.strftime("%Y-%m-%d")}')

    def _accept_cookies(self, driver):
        try:
            wait = WebDriverWait(driver, 10)
            accept_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Accept all')]")))
            accept_button.click()
            print("Accepted cookies.")
        except TimeoutException:
            print("Accept button not found or not clickable.")

    def scrape(self):
        options = webdriver.ChromeOptions()
        options.headless = True  # Set False to watch the browser actions
        with webdriver.Chrome(options=options) as driver:
            driver.get(self._make_url())

            self._accept_cookies(driver)

            try:
                # Now using the aria-label to find the element with the price
                lowest_price_element = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located(
                        (By.XPATH, "(//span[@aria-label and contains(@aria-label, 'euros')])[1]"))
                )

                lowest_price = lowest_price_element.get_attribute('aria-label')
                numeric_part = int(re.search(r'\d+', lowest_price).group())

                return numeric_part

            except TimeoutException:
                print("Failed to find the lowest price element within the given time.")
            except Exception as e:
                print(f"An error occurred: {e}")
