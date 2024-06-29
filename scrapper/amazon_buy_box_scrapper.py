
import httpx
import os
import concurrent.futures
from selectolax.parser import HTMLParser
import pandas as pd
from fake_useragent import UserAgent
import pygsheets
from bs4 import BeautifulSoup
import time
# import driver as d
import driver_setting
import numpy as np
from piTask import general
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import requests
import json
import random
import schedule

class WebDriverManager:
    def __init__(self):
      pass
        

    def configure_driver(self):
        # Generate a random User-Agent using fake_useragent
        user_agent = UserAgent().random

        options = Options()
        options.add_argument("--headless")  # Add this line to run Chrome in headless mode
        options.add_argument("driver-infobars")
        options.add_argument("start-maximized")
        options.add_argument("disable-dev-shm-usage")
        options.add_argument("no-sandbox")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("disable-blink-features=AutomationControlled")
        options.add_argument(f"--force-device-scale-factor=0.67")
        options.add_argument(f"user-agent={user_agent}")  # Set the generated User-Agent

        options.add_experimental_option(
            "prefs",
            {
                "profile.password_manager_enabled": False,
                "credentials_enable_service": False,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                # Disable images loading
                # "profile.managed_default_content_settings.images": 2,
            },
        )

        service = Service()
        driver = webdriver.Chrome(options=options, service=service)
        driver.implicitly_wait(1)
        return driver

def get_data_selenium(asn):
    driver=WebDriverManager().configure_driver()
    url = f"https://www.amazon.in/gp/product/ajax/ref=auto_load_aod?asin={asn}&pc=dp&experienceId=aodAjaxMain"
    driver.get(url)
    text=str(BeautifulSoup(driver.page_source, "html.parser"))
    html = HTMLParser(driver.page_source)
    driver.close()
    return html,text

def get_soup_selenium(asn):
    driver=WebDriverManager().configure_driver()
    url = f'https://www.amazon.in/dp/{asn}'
    driver.get(url)
    cookies = driver.get_cookies()
    text=str(BeautifulSoup(driver.page_source, "html.parser"))
    html = HTMLParser(driver.page_source)
    driver.close()
    return html,text,cookies

html,text,cookies = get_soup_selenium('B08FB396L1')
cookies_str = json.dumps(cookies)

def amazon_other_seller_api(asn,user_agent):
    url = f"https://www.amazon.in/gp/product/ajax/ref=auto_load_aod?asin={asn}&pc=dp&experienceId=aodAjaxMain"

    headers = {
      'authority': 'www.amazon.in',
      'accept': 'text/html,*/*',
      'accept-language': 'en-GB,en;q=0.8',
      'cookie':cookies_str,
      'referer': f'https://www.amazon.in/dp/{asn}/ref=olp-opf-redir?aod=1&ie=UTF8&condition=NEW',
      'sec-ch-ua': '"Not A(Brand";v="99", "Brave";v="121", "Chromium";v="121"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Linux"',
      'sec-ch-ua-platform-version': '"6.5.0"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-origin',
      'sec-gpc': '1',
      'user-agent': user_agent,
      'x-requested-with': 'XMLHttpRequest'
    }

    response = httpx.get(url, headers=headers)
    return response


def get_soup(asn,retries_httpx=10, retries_selenium=5, TimeoutError_times=3, connection_error_times=3):

    # Base condition for recursion
    if TimeoutError_times == 0 or connection_error_times == 0:
        print(f"Max retries reached for ASIN {asn}")
        return None

    

    for retry in range(retries_httpx):
        user_agent = UserAgent(os='linux').random
        
        try:
            response = amazon_other_seller_api(asn=asn,user_agent=user_agent)

            if response.status_code == 404:
                print(f'Page not found for ASIN {asn}')
                return None

            html = response.text
            if "api-services-support@amazon.com" not in html:
                return HTMLParser(html)
                
            print(f'Retrying ({retry + 1}/{retries_httpx}) due to error message for {asn}')

        except httpx.ReadTimeout as e:
            print('Timeout error occurred for URL:', asn)
            if TimeoutError_times > 0:
                return get_soup(asn, retries_httpx, retries_selenium, TimeoutError_times - 1, connection_error_times)

        except httpx.HTTPStatusError as e:
            print(f'HTTP Status Error ({e.response.status_code}) for URL:', asn)
            return None

        except httpx.ConnectError as e:
            print('Connection error occurred for URL:', asn)
            if connection_error_times > 0:
                return get_soup(asn, retries_httpx, retries_selenium, TimeoutError_times, connection_error_times - 1)
    
    for selenium_retry in range(1, retries_selenium + 1):
        print(f'Retrying with selenium ({selenium_retry}/{retries_selenium}) for {asn}')
        html, text= get_data_selenium(asn)
        if "api-services-support@amazon.com" not in text:
            print(f"data found in selenium in attempt {selenium_retry}/{retries_selenium}")
            return html

    return None


def amazon_buy_box_scrapper(asin):
    html = get_soup(asin)
    data ={}
    details  = {}
    data['ASIN'] = asin
    try:
        main_seller_css= '[id="aod-pinned-offer"]'
        main_seller_container = html.css_first(main_seller_css)
        seller_name = main_seller_container.css_first('[id="aod-offer-soldBy"] [aria-label="Opens a new page"]').text().strip()
        price = int(float(main_seller_container.css_first('[class="a-price-whole"]').text().strip().replace('₹', '').replace(',','')))

        details[seller_name] = price

    except:
        pass
    try:
        selector_value = '[id="aod-offer-list"]'
        seller_container = html.css_first(selector_value)

        no_of_other_seller = len(seller_container.css('[id="aod-offer"]'))

        for i in range(no_of_other_seller):
            seller_name = seller_container.css('[id="aod-offer-soldBy"] [aria-label="Opens a new page"]')[i].text().strip()
            price = int(float(seller_container.css('[class="a-price-whole"]')[i].text().strip().replace('₹', '').replace(',','')))
            details[seller_name] = price
    except:
        pass

    data['az_other_seller_info'] = details
    return data


def amazon_scrapper(amazon_ids):
    max_concurrent_requests = random.randint(40,50)

    # Create a ThreadPoolExecutor and use it to parallelize the scraping
    with concurrent.futures.ThreadPoolExecutor(max_concurrent_requests) as executor:
        results = list(executor.map(amazon_buy_box_scrapper, amazon_ids))

    # Filter out None results   
    results = [result for result in results if result is not None]
    if results:
        df = pd.DataFrame(results)
    else:
        df = pd.DataFrame()
        
    return df