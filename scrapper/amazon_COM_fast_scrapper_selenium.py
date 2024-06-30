import httpx
import os
import concurrent.futures
from selectolax.parser import HTMLParser
import pandas as pd
from fake_useragent import UserAgent
import pygsheets
from bs4 import BeautifulSoup
import time
import numpy as np
from piTask import general
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import requests
import json
import random

import re
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

def get_soup_selenium(asn):
    driver=WebDriverManager().configure_driver()
    url = f'https://www.amazon.com/dp/{asn}'
    driver.get(url)
    cookies = driver.get_cookies()
    text=str(BeautifulSoup(driver.page_source, "html.parser"))
    html = HTMLParser(driver.page_source)
    driver.close()
    return html,text,cookies

html,text,cookies = get_soup_selenium('B071Z8M4KX')
cookies_str = json.dumps(cookies)



selectors = [
    {
        'Price': ['[id="corePriceDisplay_desktop_feature_div"] .a-price.aok-align-center', '[id="corePrice_desktop"] tbody tr .a-offscreen'],
        'MRP': ['[id="corePriceDisplay_desktop_feature_div" ] [class="aok-relative"]','[id="corePrice_desktop"] tbody tr .a-offscreen', 'span[class="a-price a-text-price"]'],
        'Title': ['#productTitle'],
        'Rating': ['#acrPopover > span.a-declarative > a > span','#acrPopover > span.a-declarative a span'],
        'Number_of_rating': ['#acrCustomerReviewText'],
        'description_data': ['#detailBulletsWrapper_feature_div','#prodDetails','[id="productDetails_feature_div"]','[id="productOverview_feature_div"]','[id="productFactsDesktopExpander"] > div >div >div'], 
        'Deal_of_the_day':['#dealBadgeSupportingText'],
        'Coupan' : ['.promoPriceBlockMessage > div > label'],
        'az_seller_info' : ['[id="shipsFromSoldByInsideBuyBox_feature_div"]','[class="tabular-buybox-container"]','[id="offerDisplayFeatures_desktop"]'],
        'az_other_seller_info' : ['#mbc .a-row'],
        'no. of A+ blogs': ['[id="aplus"] > div > div'],
        # 'Image counts' : ['[id="altImages"] .imageThumbnail'],
        # 'video counts' : ['[id="videoCount"]'],
        'Brand Store' : ['[id="bylineInfo"'],
        'bullet points text' :  ['[id="feature-bullets"] li','[id="productFactsDesktopExpander"] li'],
        'Brand Name' : ['.po-brand td'],
        'Product description' :['[id="productDescription"]'],
        'price_per_unit' : ['.pricePerUnit .a-offscreen'],
        'image_link' : ['[id="imgTagWrapperId"] img']

    },
]




def get_soup(asn,retries_httpx=10, retries_selenium=5, TimeoutError_times=3, connection_error_times=3):

    # Base condition for recursion
    if TimeoutError_times == 0 or connection_error_times == 0:
        print(f"Max retries reached for ASIN {asn}")
        return None

    url = f'https://www.amazon.com/dp/{asn}'

    for retry in range(retries_httpx):
        try:
            if retry %2 != 0:
                user_agent = UserAgent(os='linux').random
            else:
                user_agent = UserAgent(os='linux').chrome
                
            with httpx.Client(timeout=10) as client:
                request = client.build_request("GET", url, headers={"User-Agent": user_agent, 'cookies':cookies_str})
                response = client.send(request, stream=True)
                response.read()
                response.raise_for_status()

                if response.status_code == 404:
                    print(f'Page not found for ASIN {asn}')
                    return None

                html = response.text
                if "api-services-support@amazon.com" not in html:
                    return HTMLParser(html)
                
            print(f'Retrying ({retry + 1}/{retries_httpx}) due to error message for {asn}')

        except httpx.ReadTimeout as e:
            print('Timeout error occurred for URL:', url)
            if TimeoutError_times > 0:
                return get_soup(asn, retries_httpx, retries_selenium, TimeoutError_times - 1, connection_error_times)

        except httpx.HTTPStatusError as e:
            print(f'HTTP Status Error ({e.response.status_code}) for URL:', url)
            return None

        except httpx.ConnectError as e:
            print('Connection error occurred for URL:', url)
            if connection_error_times > 0:
                return get_soup(asn, retries_httpx, retries_selenium, TimeoutError_times, connection_error_times - 1)
    
    for selenium_retry in range(1, retries_selenium + 1):
        print(f'Retrying with selenium ({selenium_retry}/{retries_selenium}) for {asn}')
        html, text,cookiee = get_soup_selenium(asn)
        if "api-services-support@amazon.com" not in text:
            print(f"data found in selenium in attempt {selenium_retry}/{retries_selenium}")
            return html

    return None

def scrape_amazon_data_details(asin):
    html = get_soup(asn=asin)
    print(f'{html} for {asin}')
    
    if html is not None:
        data ={}
        data['ASIN'] = asin
        length_list = ['no. of A+ blogs','Image counts']
        second_text_list = ['Brand Name']
        for selector in selectors:
            for key, value in selector.items():
                if key != 'description_data':
                    if isinstance(value, list):
                        for index , selector_value in enumerate(value):
                            if key == 'az_other_seller_info':
                                details = {}
                                rows = html.css(selector_value)
                                for index, row in enumerate(rows):
                                    if index %2 ==0:
                                        try:
                                            key_text = row.css_first('div div').text().split('by: ')[1].strip()
                                            value_text = row.css_first('div span').text().strip().replace('$', '').replace(',','')
                                            value_text = float(''.join(filter(lambda x: x.isdigit() or x == '.', value_text)))
                                            details[key_text] = value_text
                                        except:
                                            pass
                    
                                data[key] = details  
                                data['number of other seller']= len(details)

                            elif key == 'Price':
                                try:
                                    if index ==0:
                                        rows = html.css(selector_value)
                                        if len(rows) ==0:
                                            continue
                                    
                                        for i in range(len(rows)):
                                            try:
                                                mrp_price  = rows[i].text().strip().split('$')[-1]
                                                if mrp_price == '':
                                                    mrp_price = rows[i].text().strip().split(':')[1].split(' ')[-1].split('$')[-1]
                                                
                                                mrp_price = float(''.join(filter(lambda x: x.isdigit() or x == '.', mrp_price)))
                                                data[key] = mrp_price
                                                break
                                            except Exception as e:
                                                pass
                                    else:
                                        rows = html.css(selector_value)
                                        
                                        if len(rows)>1:
                                            mrp_price = rows[1].text().strip().split('$')[-1]
                                        else:
                                            mrp_price = rows[0].text().strip().split('$')[-1]
                                        data[key] = mrp_price
                                    break

                                except:
                                    data[key] = None   

                            elif key == 'MRP':
                                try:
                                    if index ==0:
                                        rows = html.css(selector_value)
                                        for i in range(len(rows)):
                                            try:
                                                mrp_price  = rows[i].text().strip().split(':')[1].split(' ')[1].split('$')[-1]
                                                if mrp_price == '':
                                                    mrp_price = rows[i].text().strip().split(':')[1].split(' ')[-1].split('$')[-1]
                                                
                                                mrp_price = float(''.join(filter(lambda x: x.isdigit() or x == '.', mrp_price)))
                                                data[key] = mrp_price
                                                
                                                break
                                            except Exception as e:
                                                pass
                                        
                                    else:
                                        rows = html.css_first(selector_value)
                                        mrp_price = rows.text()
                                        data[key] = mrp_price

                                    break
                                except:
                                    data[key] = None
                                    
                            elif key == 'video counts':  
                                video_no = html.css_first(selector_value)
                                if video_no:
                                    video_count = video_no.text().strip().split(' ')
                                    if len(video_count) == 1:
                                            data[key] = 1
                                    else:
                                        data[key] = video_count[0]    
                                else:
                                    data[key] = 0
                            
                            elif key == 'Brand Store':  
                                store = html.css_first(selector_value)
                                if store:
                                    store_link = store.text()
                                    data[key] = True
                                else:
                                    store_link = None
                                    data[key] = False  

                            elif key in second_text_list:  
                                brand = html.css(selector_value)
                                if brand:
                                    data[key] = brand[1].text().strip()
                                else:
                                    data[key]= None

                            
                            elif key in length_list:
                                a_plus = html.css(selector_value)
                                if a_plus:
                                    data[key] = len(a_plus) 
                                else:
                                    data[key] = 0

                            elif key == 'az_seller_info':
                                try:
                                    if index ==0:
                                        seller = html.css_first(selector_value)
                                        seller_text = seller.text().strip()
                                        
                                        if 'Sold and fulfilled by' in seller_text:
                                            seller_list = seller_text.split(',')
                                            value1 = seller_list[0].split(' by ')
                                            data['Sold_by'] = value1[1].rstrip('.')
                                            data['warehouse_status'] = value1[1].rstrip('.')
                                        # Check if "Sold by" is present
                                        elif 'Sold by' in seller_text:
                                            seller_list = seller_text.split(' and ')
                                            key1, value1 = seller_list[0].split(' by ')
                                            data['Sold_by'] = value1.rstrip('.')
                                            data['warehouse_status'] = seller_list[1].rstrip('.')
                                        break
                                    else:
                                        seller = html.css_first(selector_value)
                                        seller_text = seller.text().strip()
                                        lines = seller_text.split('\n')
                                        filtered_lines = [line.strip() for line in lines if any(char.isalpha() for char in line)]
                                        data['warehouse_status'] = f'{filtered_lines[0]} {filtered_lines[1]}'
                                        data['Sold_by'] =  filtered_lines[4]
                                    break
                                except:
                                    data['Sold_by'] = None
                                    data['warehouse_status'] =None


                            elif key == 'bullet points text':
                                    bullet_TEXT = []
                                    try:
                                        rows = html.css(selector_value)
                                        for i in range(len(rows)):
                                            try:
                                                point_text  = rows[i].text()
                                                bullet_TEXT.append(point_text)

                                            except Exception as e:
                                                pass
                                        
                                        if len(rows) != 0:
                                            data['no of bullet points'] = len(rows)
                                            data[key] = bullet_TEXT
                                            break
                                    except Exception as e:
                                        data['no of bullet points'] = 0
                                        data[key] = bullet_TEXT

                            elif key == 'image_link':
                                try:
                                    img = html.css_first(selector_value)
                                    imgs = img.attributes['src']
                                    data[key] = imgs
                                except Exception as e:
                                    data[key] = ''

                            else:
                                elements = []
                                for element in html.css(selector_value):
                                    if element:
                                        elements.append(element.text().strip())
                                    else:
                                        pass

                                if elements:
                                    data[key] = elements[0] if elements else ''
                                    break
                                else:
                                    data[key] = None

        description_data = {}
        for desc_selector in selector['description_data']:
            is_description_parents = html.css_matches(desc_selector)
            
            if is_description_parents:
                if desc_selector == '#prodDetails':
                    description_parent = html.css_first(desc_selector)
                    if description_parent:
                        
                        description_children = description_parent.css('tr')
                        for child in description_children:
                            key_element = child.css_first('th')
                            value_element = child.css_first('td')
                            if key_element and value_element:
                                key_text = key_element.text().strip().split(":")[0].strip().replace("‏", "").replace(":","").strip().lower().replace('\u200e',"")
                                value_text = value_element.text().strip().split(":")[0].strip().replace("‏", "").replace(":","").strip().lower().replace('\u200e',"")
                                description_data[key_text] = value_text
                elif desc_selector == '#detailBulletsWrapper_feature_div':
                    description_parent = html.css_first(desc_selector)
                    if description_parent:
                        description_children = description_parent.css('li')
                        for child in description_children:
                            key_element = child.css_first('span.a-text-bold')
                            value_element = child.css_first('span:nth-child(2)')
                            try:
                                if key_element and value_element:
                                    key_text = key_element.text().strip().split(":")[0].strip().replace("‏", "").replace(":","").strip().lower().replace('\u200e',"").strip()
                                    value_text = value_element.text().strip().split(":")[0].strip().replace("‏", "").replace(":","").strip().lower().replace('\u200e',"").strip()
                                    description_data[key_text] = value_text
                                else:
                                    key_text= html.css_first('#detailBulletsWrapper_feature_div > ul >li >span >span').text().replace("‏","").replace(":","").strip().lower()
                                    value_text=html.css_first("#detailBulletsWrapper_feature_div > ul >li >span").text().replace("Best Sellers Rank: ","").strip()
                                    description_data[key_text] = value_text
                            except:
                                pass

                elif desc_selector == '[id="productFactsDesktopExpander"] > div >div >div':
                    try:
                        rows = html.css(desc_selector)
                        if len(rows) >0:
                            for i in range(len(rows)):
                                key = rows[i].css('div > span')[0].text().strip()
                                value = rows[i].css('div > span')[1].text().strip()
                                description_data[key] = value
                    except:
                        pass
                else:
                    description_parent = html.css_first(desc_selector)
                    if description_parent:
                        description_children = description_parent.css('table tr')
                        for child in description_children:
                            try:
                                key_element, value_element = [child.css('td')[i] for i in range(2)]
                                if key_element and value_element:
                                    key_text = key_element.text().strip().split(":")[0].strip().replace("‏", "").replace(":","").strip().lower().replace('\u200e',"").strip()
                                    value_text = value_element.text().strip().split(":")[0].strip().replace("‏", "").replace(":","").strip().lower().replace('\u200e',"").strip()
                                    description_data[key_text] = value_text
                            except:
                                pass

        if len(description_data) >0:
            Best_seller_rank=description_data.get('best sellers rank')
            try:
                Best_seller_rank=Best_seller_rank.split("#")[2].split('in')[0]
            except:
                Best_seller_rank=Best_seller_rank
            try:
                if description_data.get('brand') is not None:
                    data['Brand Name'] = str(description_data.get('brand')).upper()

                if data['Brand Name'] is None:
                    if store_link is not None:
                        input_string = store_link
                        patterns = [r'(\w+)\s+Store', r'Brand:\s+(.+)']
                        for pattern in patterns:
                            match = re.search(pattern, input_string)
                            if match:
                                word_before_store = match.group(1)
                                break

                        data['Brand Name'] = word_before_store.upper()
                    else:
                        data['Brand Name'] = None
            except:
                data['Brand Name'] = None

            try:
                net_quantity = description_data.get('net quantity')
                net_quantity = int(float(net_quantity.split(' ')[0]))
            except:
                net_quantity = None
                
            main_asn = description_data.get('asin')

        else:
            Best_seller_rank = None
            main_asn = data['ASIN']
            net_quantity = None
      
        return {**data, 'description_data': description_data,"Best_seller_rank":Best_seller_rank,'main_asn':main_asn,'net_quantity':net_quantity} if description_data else {}      

def amazon_scrapper(amazon_ids):
    max_concurrent_requests = random.randint(30,40)

    # Create a ThreadPoolExecutor and use it to parallelize the scraping
    with concurrent.futures.ThreadPoolExecutor(max_concurrent_requests) as executor:
        results = list(executor.map(scrape_amazon_data_details, amazon_ids))

    # Filter out None results   
    results = [result for result in results if result is not None]
    if results:
        df = pd.DataFrame(results)

        if 'MRP' in df.columns:
            try:
                df['MRP'] = pd.to_numeric(df['MRP'].astype(str).replace('', pd.NA).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0).astype(int)
            except:
                df['MRP'] = pd.to_numeric(df['MRP'].astype(str).replace('', pd.NA).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0).astype(float)

        if 'Price' in df.columns:        
            try:
                df['Price'] = pd.to_numeric(df['Price'].astype(str).replace('', pd.NA).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0).astype(int)
            except:
                df['Price'] = pd.to_numeric(df['Price'].astype(str).replace('', pd.NA).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0).astype(float)
        try:
            df['net_quantity'] = pd.to_numeric(df['net_quantity'], errors='coerce')
            # df['net_quantity'] = df['net_quantity'].astype(float).astype(int)
            # df['price_per_unit'] = np.where(df['net_quantity'].isna(), np.nan, round(df['Price'] / df['net_quantity'], 2))
        except:
            pass
        try:
            df['price_per_unit'] = np.where(df['price_per_unit'].isna(), np.nan, df['price_per_unit'].str.replace('$',''))
        except:
            pass
        try:
            df['Rating'] = df['Rating'].astype(str).str.split(' ').str[0]
            df['Number_of_rating'] =df['Number_of_rating'].str.split(' ').str.get(0)
            df['Coupan'] = df['Coupan'].str.extract(r'(\d+%)')
        except:
            pass
        
        try:
            cols = list(df.columns)
            if 'MRP' in cols:
                cols.remove('MRP')
                cols.insert(2, 'MRP')
            cols.remove('price_per_unit') 
            cols.append('price_per_unit')  
            df = df[cols]
        except:
            pass
        df.drop_duplicates(subset=['ASIN'], inplace=True)
        final_df =df.copy()
        if(final_df is not None):
            az_scrapper.amazon_scraping_data(final_df)
    else:
        df = pd.DataFrame()

    return  df

