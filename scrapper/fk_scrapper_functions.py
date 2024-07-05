from piTask import general
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from scrapper import flipkart_json_scrapper_with_all_specifications as fk_scrapper
import pandas as pd
pd.set_option('display.max_columns', None)
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import numpy as np


def open_chrome_headless():
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    # driver = webdriver.Remote(command_executor="http://localhost:4444/wd/hub",options=options)
    return driver


def fns_scrap(driver,url,no_of_pages):
    fsns = []
    for i in range(1, no_of_pages + 1):
        final_url = url + f'&page={i}'
        print(final_url)
        driver.get(final_url)
        all_fsns = driver.find_elements(By.XPATH, '//*[@data-id]')
        for fsn in all_fsns:
            single_fsn = fsn.get_attribute('data-id')
            fsns.append(single_fsn)        
    return fsns