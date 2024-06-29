import httpx
import os
import concurrent.futures
from selectolax.parser import HTMLParser
import pandas as pd
from fake_useragent import UserAgent
import pygsheets
from piTask import general

# spreadsheet_id="1MnMNwiN8qEIXnOpEVi7pbS5cIQDwANYDRfjk1WoKwks"
# input_sheet_name="mapping"  
# current_dir = os. getcwd()
# service_file_path = os.path.join(current_dir, 'key.json')
# gc=pygsheets.authorize(service_file =service_file_path)
# sh=gc.open_by_key(spreadsheet_id)
# input_selected_sheet=sh.worksheet_by_title(input_sheet_name)
# df=input_selected_sheet.get_as_df()


# fsns = df['FSN'].to_list()
# print(len(fsns))
selectors = [
    {
        'Title': ['.aMaAEs .B_NuCI'],
        'Price': ['.aMaAEs div._16Jk6d','._30jeq3._16Jk6d','._16Jk6d'],
        'MRP': [
            '.aMaAEs ._2p6lqe'
        ],
        'Brand': ['._3dtsli'],
        'Brand_name':['.G6XhRU'],
        'Rating': ['.aMaAEs div._3LWZlK'],
        'Number_of_rating': ['.aMaAEs span._2_R_DZ'],
        'Price_per_unit': ['.aMaAEs div.V_omJD'],
        # 'description_data': ['#detailBulletsWrapper_feature_div','#prodDetails'], 
        # 'Deal_of_the_day':['#dealBadgeSupportingText'],
        # 'Coupan' : ['.promoPriceBlockMessage > div > label']

    },
]


def scrape_flipkart_data(fsn, max_retries=3):
    print(fsn)
    user_agent = UserAgent().random
    headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Host": "www.flipkart.com",
    "Sec-Ch-Ua": '"Microsoft Edge";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
    "Sec-Ch-Ua-Arch": "x86",
    "Sec-Ch-Ua-Full-Version": "117.0.2045.55",
    "Sec-Ch-Ua-Full-Version-List": '"Microsoft Edge";v="117.0.2045.55", "Not;A=Brand";v="8.0.0.0", "Chromium";v="117.0.5938.150"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Model": "",
    "Sec-Ch-Ua-Platform": "Linux",
    "Sec-Ch-Ua-Platform-Version": "6.2.0",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": user_agent 
    }

# Add cookies
    cookies = {
        "Network-Type": "4g",
        "T": "SD.9b722a5e-9c8f-4198-850d-dc0ca9215661.1693206302100",
        "K-ACTION": "null",
        "_pxvid": "57390a43-4729-11ee-abef-72983317a48f",
        "dpr": "1",
        "_ga_2P94RMW04V": "GS1.2.1696313865.4.0.1696313865.0.0.0",
        "at": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImQ2Yjk5NDViLWZmYTEtNGQ5ZC1iZDQyLTFkN2RmZTU4ZGNmYSJ9.eyJleHAiOjE2OTgwNDc1MjEsImlhdCI6MTY5NjMxOTUyMSwiaXNzIjoia2V2bGFyIiwianRpIjoiZGUyYTM3MTEtYzU3OS00ZWM3LWJiYjItMDE1ZTJmYTFlYzNjIiwidHlwZSI6IkFUIiwiZElkIjoiU0QuOWI3MjJhNWUtOWM4Zi00MTk4LTg1MGQtZGMwY2E5MjE1NjYxLjE2OTMyMDYzMDIxMDAiLCJrZXZJZCI6IlZJOTcwQzE4QjZDQzlDNDAwN0I5RDYzNDIzOTAzNUQzMjYiLCJ0SWQiOiJtYXBpIiwidnMiOiJMTyIsInoiOiJDSCIsIm0iOnRydWUsImdlbiI6NH0.xkUv5A-2sQtoxUOKkCFP3ZTTe4xL15-EiRkpWJ5Lxy8",
        # ... add other cookies as needed
    }
    url = f'https://www.flipkart.com/continental-xtra-instant-coffee/p/itm7a1d2610e8e3d?pid={fsn}&lid=LSTCFEF3CNBJBC8UWSMVEAM4M&marketplace=FLIPKART&store=eat%2Fdui&srno=b_1_6&otracker=browse&fm=organic&iid=83a8a753-5879-4b27-8632-8a75e6c28d91.CFEF3CNBJBC8UWSM.SEARCH&ppt=browse&ppn=browse&ssid=h9pc5f0stc0000001696319525191'
    
    
    for _ in range(max_retries):
        try:
            resp = httpx.get(url, headers=headers,cookies=cookies)
            resp.raise_for_status()  # Raise an exception for HTTP errors
            html = HTMLParser(resp.text)
            data = {}
            for selector in selectors:
                data['fsn'] = fsn
                for key, value in selector.items():
                    if isinstance(value, list):
                        elements = [element.text().strip() for selector_value in value for element in html.css(selector_value)]
                        data[key] = elements[0] if elements else ''
                    else:
                        element = html.css_first(value)
                        data[key] = element.text().strip() if element else ''
            try:
                Brand = data['Brand'].split("Brand")[1].split("Model")[0]
            except:
                Brand = ""
            try:
                Number_of_rating = data['Number_of_rating'].split("Rating")[0].strip()
            except:
                Number_of_rating = ""
            data['Number_of_rating'] = Number_of_rating
            data['Brand'] = Brand
            return data
        except httpx.HTTPError as e:
        
            if _ == max_retries - 1:  
                raise
            else:
                continue  # Try again



def scrape_all_fsns(fsns, max_retries=3):
    scraped_data = []  
    failed_fsns = []  
    
    max_concurrent_requests = 300
    
    # Create a ThreadPoolExecutor to parallelize the scraping
    with concurrent.futures.ThreadPoolExecutor(max_concurrent_requests) as executor:
        future_to_fsn = {executor.submit(scrape_flipkart_data, fsn, max_retries): fsn for fsn in fsns}
        
        for future in concurrent.futures.as_completed(future_to_fsn):
            fsn = future_to_fsn[future]
            try:
                data = future.result()  # Get the scraped data for the FSN
                scraped_data.append(data)
            except Exception as e:

                failed_fsns.append(fsn)
    

    df = pd.DataFrame(scraped_data)

    return df, failed_fsns  

# creating a loop to check for fsns where result is not present
# store in empty list called out_df
# concat the final result
def flipkart_scrapper(fsns):
    df,failed=scrape_all_fsns(fsns)
    out_df=[]
    df1=df[~(df['Title']=='')]
    out_df.append(df1)
    fsns=df[(df['Title']=='')]['fsn'].to_list()

    len_previous_fsn=len(fsns)
    while(len(fsns)>1):
        df,failed=scrape_all_fsns(fsns)
        df1=df[~(df['Title']=='')]
        out_df.append(df1)
        fsns=df[(df['Title']=='')]['fsn'].to_list()
        if len_previous_fsn == len(fsns):
            break
        print(len(fsns))
        len_previous_fsn=len(fsns)

    final_df=pd.concat(out_df)

    final_df['Price'] = final_df['Price'].str.replace("₹", "").str.replace(",", "")
    final_df['MRP'] = final_df['MRP'].str.replace("₹", "").str.replace(",", "")
    
    return  final_df
# general.print_sheet(final_df,'fk_scraped_live_price',sh,1,1)