from fake_useragent import UserAgent
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser
import json
import httpx
import random
import concurrent.futures
import pandas as pd


class WebDriverManager:
    def __init__(self):
        pass

    def configure_driver(self):
        # Generate a random User-Agent using fake_useragent
        user_agent = UserAgent().random

        options = Options()
        # Add this line to run Chrome in headless mode
        options.add_argument("--headless")
        options.add_argument("driver-infobars")
        options.add_argument("start-maximized")
        options.add_argument("disable-dev-shm-usage")
        options.add_argument("no-sandbox")
        options.add_experimental_option(
            "excludeSwitches", ["enable-automation"])
        options.add_argument("disable-blink-features=AutomationControlled")
        options.add_argument(f"--force-device-scale-factor=0.67")
        # Set the generated User-Agent
        options.add_argument(f"user-agent={user_agent}")

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


def collecting_data_from_nykaa_man(id):
    url = f'https://www.nykaaman.com/jbl-boombox-2-wireless-portable-bluetooth-speaker-24hrs-playtime-powerful-bass-ipx7-black/p/{id}?productId=7503339&pps=1'

    user_agent = UserAgent(os='linux').random

    def get_soup_selenium():
        driver = WebDriverManager().configure_driver()
        url = f'https://www.nykaaman.com/jbl-boombox-2-wireless-portable-bluetooth-speaker-24hrs-playtime-powerful-bass-ipx7-black/p/{id}?productId=7503339&pps=1'
        driver.get(url)
        cookies = driver.get_cookies()
        text = str(BeautifulSoup(driver.page_source, "html.parser"))
        html = HTMLParser(driver.page_source)
        driver.close()
        return html, text, cookies

    html, text, cookies = get_soup_selenium()
    cookies_str = json.dumps(cookies)

    with httpx.Client(timeout=10) as client:
        request = client.build_request(
            "GET", url, headers={"User-Agent": user_agent, 'cookies': cookies_str})
        response = client.send(request, stream=True)
        response.read()
        html = response.text

    def convert_to_json_with_script_id(text):
        soup = BeautifulSoup(text, 'html.parser')
        script_tag = soup.find(
            'script', text=lambda t: t and '__PRELOADED_STATE__' in t)

        if script_tag:
            try:
                script_content = script_tag.string
                start_index = script_content.find('{')
                end_index = script_content.rfind('}') + 1
                json_data = script_content[start_index:end_index]
                parsed_data = json.loads(json_data)
                return parsed_data
            except Exception as e:
                print(f"Error parsing JSON data: {e}")
                return None
        else:
            print("Script tag with id '__PRELOADED_STATE__' not found")
            return None
    parsed_data = convert_to_json_with_script_id(text)
    return parsed_data


def brand_name(data):
    brand_name = data["jsonLdData"][0]["brand"]
    return brand_name


def title(data):
    title = data["jsonLdData"][0]["name"]
    return title


def selling_price(data):
    in_stock = data["dataLayer"]["product"]["inStock"]
    if in_stock:
        selling_price = data["jsonLdData"][0]["offers"]["price"]
    else:
        selling_price = "OOS"
    return selling_price


def mrp(data):
    mrp = data["dataLayer"]["product"]["mrp"]
    return mrp


def rating(data):
    try:
        rating = data["productPage"]["product"]["rating"]
    except KeyError:
        rating = None
    return rating


def rating_count(data):
    try:
        rating_count = data["productPage"]["product"]["ratingCount"]
    except KeyError:
        rating_count = None
    return rating_count


def review_count(data):
    try:
        review_count = data["productPage"]["product"]["reviewCount"]
    except KeyError:
        review_count = None
    return review_count


def nykaa_man_scrapper(id):
    product_details_dict = {}
    data = collecting_data_from_nykaa_man(id)
    product_details_dict["Id"] = id
    product_details_dict["Brand Name"] = brand_name(data)
    product_details_dict["Title"] = title(data)
    product_details_dict["Rating"] = rating(data)
    product_details_dict["Rating Count"] = rating_count(data)
    product_details_dict["Selling Price"] = selling_price(data)
    product_details_dict["MRP"] = mrp(data)

    return product_details_dict


def nykaa_man_scrap(ids):
    max_concurrent_requests = random.randint(30, 40)

    # Create a ThreadPoolExecutor and use it to parallelize the scraping
    with concurrent.futures.ThreadPoolExecutor(max_concurrent_requests) as executor:
        results = list(executor.map(nykaa_man_scrapper, ids))

    # Filter out None results
    results = [result for result in results if result is not None]
    if results:
        df = pd.DataFrame(results)
        return df
