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

import http.client
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
import threading
import pandas as pd
import gzip
import io
from scrapper import flipkart_json_scrapper_with_all_specifications as fk_scrapper
from piTask import general
import ast
import re
import numpy as np
from fake_useragent import UserAgent
ua = UserAgent()
def open_chrome_headless():
    options = Options()
    options.add_argument("--headless")
    # try:
    #     driver = webdriver.Chrome(options=options)
    # except:
    driver = webdriver.Remote(command_executor="http://localhost:4444/wd/hub",options=options)
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
thread_local = threading.local()
def get_connection():
    if not hasattr(thread_local, 'conn'):
        thread_local.conn = http.client.HTTPSConnection("www.flipkart.com", timeout=10)
    return thread_local.conn

def get_fsn(search, page):
    params = {
        'q': search,
        'page': page
    }
    query_string = urlencode(params)
    path = f"/search?{query_string}"
    headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,en-IN;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Cookie': 'T=TI171834590882700170348350689694114812841536586598665351035628902266; _pxvid=eb18d238-2a15-11ef-8332-14f84e743341; vw=1825; dpr=1; ULSN=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJjb29raWUiLCJhdWQiOiJmbGlwa2FydCIsImlzcyI6ImF1dGguZmxpcGthcnQuY29tIiwiY2xhaW1zIjp7ImdlbiI6IjIiLCJ1bmlxdWVJZCI6IlVVSTI0MDYyMDExNTY1MTc4NDNPSjZNUDgiLCJma0RldiI6bnVsbH0sImV4cCI6MTczNTM5NTI2MSwiaWF0IjoxNzE5NjE1MjYxLCJqdGkiOiI5ZDExMTY5Mi03Y2UzLTRhMWUtYTA1Yy00ODhhZmNjNGFlOTEifQ.zOExzRJ7a938XyR0l0YYDIJXAWZrdo-X_3CVzJ0Yllk; ud=2.u2YKt5hrZGXNxNErz_Q2VW8FPyxCcsau8k1QHbiRYvk1M2SDMWrqCY8MD3TmoSVYSxXjwN6aD6dCfcJOgYrZ0Kx92e6InzYfjmWi43c5YcrfEW4fi15IEDoyrFMOhg_J9hTN9FxdR8s8IDJK2MS6hYoh9u_GswCVpF86xnSr1IAIE3GYBd_LDvGSC_6jDIKSY5MeCZYhN6VFehjLF7U8vZif2wMdXsOIGeNpZhzEHK0IDd4YmBulQQSaH2OO-MRI; _fbp=fb.1.1723806825233.442773842490427820; s_nr=1723806878808-Repeat; AMCV_55CFEDA0570C3FA17F000101%40AdobeOrg=-227196251%7CMCIDTS%7C19962%7CMCMID%7C77060089323537194633849373282035801268%7CMCAAMLH-1724411625%7C12%7CMCAAMB-1724663150%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1723814025s%7CNONE%7CMCAID%7CNONE; mp_9ea3bc9a23c575907407cf80efd56524_mixpanel=%7B%22distinct_id%22%3A%20%22ACC9644F91FB39448188106BA68C24709F8D%22%2C%22%24device_id%22%3A%20%221904f6797d98b5-08cc87a0a28c2b-4c657b58-1fa400-1904f6797dcaa3%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%2C%22%24user_id%22%3A%20%22ACC9644F91FB39448188106BA68C24709F8D%22%7D; _ga_0SJLGHBL81=GS1.1.1724810832.8.0.1724810832.0.0.0; _ga_TVF0VCMCT3=GS1.1.1724810832.8.0.1724810832.60.0.0; _ga=GA1.2.10499125.1718980240; _ga_2P94RMW04V=GS1.2.1725448195.1.0.1725448195.0.0.0; vh=956; AMCV_17EB401053DAF4840A490D4C%40AdobeOrg=-227196251%7CMCIDTS%7C20006%7CMCMID%7C52021146135361344382423900263871368379%7CMCAAMLH-1728625898%7C3%7CMCAAMB-1729057779%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1728460179s%7CNONE%7CMCAID%7CNONE; _gcl_au=1.1.680070548.1728456988; K-ACTION=null; at=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjNhNzdlZTgxLTRjNWYtNGU5Ni04ZmRlLWM3YWMyYjVlOTA1NSJ9.eyJleHAiOjE3Mjg0OTA3ODIsImlhdCI6MTcyODQ4ODk4MiwiaXNzIjoia2V2bGFyIiwianRpIjoiM2FlZmUyNTYtZDA0Mi00MzNkLWI5NGQtMDg3MTg2ZDAxZmUyIiwidHlwZSI6IkFUIiwiZElkIjoiVEkxNzE4MzQ1OTA4ODI3MDAxNzAzNDgzNTA2ODk2OTQxMTQ4MTI4NDE1MzY1ODY1OTg2NjUzNTEwMzU2Mjg5MDIyNjYiLCJiSWQiOiJXSERQT0YiLCJrZXZJZCI6IlZJRUI0Q0QxMTVDNUZFNDNGNEFGNDUyMDgzNTg2QzQ1RjMiLCJ0SWQiOiJtYXBpIiwiZWFJZCI6IlVSalBra2w5SmNqVUs0U3E5NlpkMWN4UWMybWduaHNXV2tWa3FxNFhPZXg2THJjcnBFTmRvdz09IiwidnMiOiJMSSIsInoiOiJDSCIsIm0iOnRydWUsImdlbiI6NH0.U0i-Lv3G0HKMjt9cQc90O_DaEXfORzMViDhmAA57Lgs; rt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjhlM2ZhMGE3LTJmZDMtNGNiMi05MWRjLTZlNTMxOGU1YTkxZiJ9.eyJleHAiOjE3NDQyMTM3ODIsImlhdCI6MTcyODQ4ODk4MiwiaXNzIjoia2V2bGFyIiwianRpIjoiZjhkYjk1NzQtMDkyNy00MTE4LTk1NjUtYTFlNDNkYzA2NTFhIiwidHlwZSI6IlJUIiwiZElkIjoiVEkxNzE4MzQ1OTA4ODI3MDAxNzAzNDgzNTA2ODk2OTQxMTQ4MTI4NDE1MzY1ODY1OTg2NjUzNTEwMzU2Mjg5MDIyNjYiLCJiSWQiOiJXSERQT0YiLCJrZXZJZCI6IlZJRUI0Q0QxMTVDNUZFNDNGNEFGNDUyMDgzNTg2QzQ1RjMiLCJ0SWQiOiJtYXBpIiwibSI6eyJ0eXBlIjoibiJ9LCJ2IjoiRVVaMFhPIn0.RnavdI1eoACinVlH0JYjSgLqPK5ms-qbtI5t_N6bVCk; vd=VIEB4CD115C5FE43F4AF452083586C45F3-1718864841660-60.1728488982.1728488982.159438843; Network-Type=4g; qH=418696e663e903e6; s_sq=flipkart-prd%3D%2526pid%253Dwww.flipkart.com%25253Asearch%2526pidt%253D1%2526oid%253Dhttps%25253A%25252F%25252Fwww.flipkart.com%25252F%2526ot%253DA; fonts-loaded=en_loaded; isH2EnabledBandwidth=true; h2NetworkBandwidth=9; gpv_pn=HomePage; gpv_pn_t=FLIPKART%3AHomePage; S=d1t10Nz8/Pz8pbD8/Pxo/fD8iWFuBy3O0csqf8iMaXX03mKhIzCKaDVTPJZQGnlIVodlGW5eg0Gtlbq1EU++yHoVQ/g==; SN=VIEB4CD115C5FE43F4AF452083586C45F3.TOKBA2590E72609430E8A8F39668EA3CEE4.1728488998999.LI',
    'Referer': 'https://www.flipkart.com/search?q=bluetooth+earphone&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&p%5B%5D=facets.price_range.from%3D1000&p%5B%5D=facets.price_range.to%3D1500',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': ua.random,
    'sec-ch-ua': '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-full-version': '"129.0.2792.79"',
    'sec-ch-ua-full-version-list': '"Microsoft Edge";v="129.0.2792.79", "Not=A?Brand";v="8.0.0.0", "Chromium";v="129.0.6668.90"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"'
    }

    try:
        conn = get_connection()
        conn.request("GET", path, headers=headers)
        res = conn.getresponse()
        if res.status != 200:
            print(f"Error fetching page {page} for search '{search}': {res.status} {res.reason}")
            res.close()
            return []
        data = res.read()
        res.close()

        # Handle gzip encoding if present
        encoding = res.getheader('Content-Encoding')
        if encoding == 'gzip':
            buf = io.BytesIO(data)
            f = gzip.GzipFile(fileobj=buf)
            data = f.read()

        soup = BeautifulSoup(data, 'html.parser')
        elements_with_data_id = soup.find_all(attrs={'data-id': True})
        data_list = []
        for position, element in enumerate(elements_with_data_id, start=1):
            data_id = element['data-id']
            data_list.append({
                'search_query': search,
                'position': position,
                'page_no': page,
                'data_id': data_id
            })

        return data_list

    except Exception as e:
        print(f"Exception in get_fsn for search '{search}' page {page}: {e}")
        return []

def collect_all_data_ids(search_queries, start_page=1, end_page=25):
    all_data = []
    tasks = [(search, page) for search in search_queries for page in range(start_page, end_page + 1)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_task = {executor.submit(get_fsn, search, page): (search, page) for (search, page) in tasks}

        for future in concurrent.futures.as_completed(future_to_task):
            search, page = future_to_task[future]
            try:
                data_list = future.result()
                all_data.extend(data_list)
            except Exception as exc:
                print(f"Search '{search}' Page {page} generated an exception: {exc}")

    return all_data