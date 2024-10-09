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

# Create an instance of the UserAgent class
ua = UserAgent()

# Thread-local data to store per-thread HTTPSConnection
thread_local = threading.local()
output_sheet = '1AZVMYH_qgty_0IT8TtmsCZypWisTn4yOHdCM_JdghG8'

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

def collect_all_data_ids(search_queries, start_page, end_page):
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

search_term = general.read_sheet(output_sheet, "search", 1)
search_queries = search_term['search_term'].to_list()
start_page = 1
end_page = 25

all_data = collect_all_data_ids(search_queries, start_page, end_page)
print(f"Total data-id attributes found: {len(all_data)}")

# Create DataFrame
search_term_page = pd.DataFrame(all_data, columns=['search_query', 'position', 'page_no', 'data_id'])
general.add_sheet_name(output_sheet, 'search_output', 1)
general.print_sheet(1, search_term_page, 'search_output', output_sheet, 1, 1, 1)

all_fsns = search_term_page['data_id'].to_list()
final_fsn_list = list(set(all_fsns))
competitor_data = fk_scrapper.scrape_all_fsns(final_fsn_list)
competitor_data['brand'] = competitor_data['title'].str.split(" ").str[0]

self = general.read_sheet(output_sheet, sheet_name='self')
self_fsn = self['fsn'].to_list()
self_data = fk_scrapper.scrape_all_fsns(self_fsn)

def convert_to_all_columns(brand_level_data):
    # Collect dictionaries for each row in the DataFrame
    rows_to_expand = []

    for index, row in brand_level_data.iterrows():
        specs = row['all_specs']

        if not isinstance(specs, dict):
            try:
                # Attempt to convert if specs is a string representation of a dictionary
                specs = ast.literal_eval(specs)
            except (ValueError, SyntaxError):
                continue

        # Only process if it's a valid dictionary
        if isinstance(specs, dict):
            rows_to_expand.append(pd.Series(specs, name=index))

    # Create a new DataFrame from the extracted specs and merge it
    if rows_to_expand:
        specs_df = pd.DataFrame(rows_to_expand)
        brand_level_data = pd.concat([brand_level_data, specs_df], axis=1)

    return brand_level_data


competitor_data = convert_to_all_columns(competitor_data)
self_data = convert_to_all_columns(self_data)
competitor_data = competitor_data[competitor_data['ratings_count'] != ""]
# Create an empty dictionary to store column statistics
d = {}

# Calculate unique values, percentage of non-null values, and the unique value list for each column
for column in competitor_data.columns:
    try:
        unique_count = competitor_data[column].nunique()
        not_na_percentage = competitor_data[column].notna().mean() * 100
        unique_values_list = competitor_data[column].dropna().unique().tolist()
        d[column] = [unique_count, not_na_percentage, unique_values_list]
    except Exception:
        competitor_data[column] = competitor_data[column].astype(str)
        unique_count = competitor_data[column].nunique()
        not_na_percentage = competitor_data[column].notna().mean() * 100
        unique_values_list = competitor_data[column].dropna().unique().tolist()
        d[column] = [unique_count, not_na_percentage, unique_values_list]



# Convert the dictionary into a DataFrame
column_stats_df = pd.DataFrame.from_dict(d, orient='index', columns=['Unique Values', '% Not NaN', 'Unique Values List'])
column_stats_df.reset_index(inplace=True)
column_stats_df.rename(columns={'index': 'Column Name'}, inplace=True)

df = column_stats_df
not_to_consider_columns = [
    'productDescription', 'productImagesCount', 'productVideosCount', 'Country of Origin', 'flipkart_assured', 'special_price',
    'title', 'rating', 'ratings_count', 'reviews_count', 'Seller Name', 'highlights', 'description', 'specifications', 'reviews',
    'image_link', 'all_specs', 'brand', 'Model Name', 'mrp','Model ID'
]


df = df[(df['Unique Values'] != df['Unique Values'].max()) & (df['Unique Values'] != 1) & (~df['Column Name'].isin(not_to_consider_columns))]

def clip_to_max(x):
    x=str(x)
    if len(x)>49990:
        return x[:49990]
    else:
        return x
df['Unique Values List']=df['Unique Values List'].apply(clip_to_max)
df=df.sort_values(by='% Not NaN',ascending=False)
general.print_sheet(1, df, 'research_of_attributes', output_sheet, 1, 1, 1)

input_df = general.read_sheet(output_sheet, 'attribute', 1)
def extract_number(text):
    numbers=re.findall(r'\d+',str(text))
    if len(numbers) != 0:
        ans=max(numbers)
    else:
        ans=0
    return ans

# function is created to format data 
competitor_data['Brand']=competitor_data['title'].str.split(" ").str[0]
self_data['Brand']=self_data['title'].str.split(" ").str[0]
dynamic_range_vars={}
dynamic_categorical_values={}
for index,row in input_df.iterrows():
    if row['type'] == 'numerical':
        if row['range'] == '':
            competitor_data[row['column_name']]=competitor_data[row['column_name']].apply(extract_number)
            self_data[row['column_name']]=self_data[row['column_name']].apply(extract_number)
        if row['range'] != "":
            try:
                
                if float(row['range']) <=1:
                    # created bounds
                    bound_variable=row['column_name']
                    range_value=float(row['range'])

                    dynamic_range_vars[bound_variable+'_lower_bound']= 1-range_value
                    dynamic_range_vars[bound_variable+'_upper_bound'] = 1+range_value
                if float(row['range']) >1:
                    dynamic_range_vars[row['column_name']+'_greater_than'] = row['range']
            except:
                
                value=[int(i.strip()) for i in  row['range'].split(",")]
                value.append(np.inf)
                new_column=row['column_name']+'_bin'
                try:
                    competitor_data[row['column_name']]=competitor_data[row['column_name']].apply(extract_number)
                    self_data[row['column_name']]=self_data[row['column_name']].apply(extract_number)
                except:
                    pass
                print(new_column)
                competitor_data=competitor_data[competitor_data[row['column_name']] != ""]
                competitor_data[row['column_name']]=competitor_data[row['column_name']].astype('float')
                competitor_data[new_column]=pd.cut(competitor_data[row['column_name']],bins=value)
                competitor_data[new_column]=pd.cut(competitor_data[row['column_name']],bins=value)

                self_data=self_data[self_data[row['column_name']] != ""]
                self_data[row['column_name']]=self_data[row['column_name']].astype('float')
                self_data[new_column]=pd.cut(self_data[row['column_name']],bins=value)
                self_data[new_column]=pd.cut(self_data[row['column_name']],bins=value)

            competitor_data=competitor_data[competitor_data[row['column_name']] != ""]
            competitor_data[row['column_name']]=competitor_data[row['column_name']].astype('float')

    elif row['type'] == 'categorical':
        if row['range'] == '':
            dynamic_categorical_values[row['column_name']] = 'exact'
        else:
            values=[i.strip() for i in row['range'].split(",")]
            dynamic_categorical_values[row['column_name']] = values

self_data=self_data[self_data['final_selling_price'] != ""]

final_df_list=[]
for self_index,self_row in self_data.iterrows():
    print(self_row['fsn'])
    temp=competitor_data.copy()
    for input_index,input_row in input_df.iterrows():
        process=input_row['column_name']
        print(process)


    # for comp_index,comp_row in competitor_data.iterrows():
        if input_row['type'] == 'numerical':
            if input_row['range'] == '':
                # range_bin_column=input_row['column_name']+'_bin'
                pass
            else:
                try:
                    if float(input_row['range'])<=1:
                        range_column=input_row['column_name']
                        lower_range=dynamic_range_vars[f'{range_column}_lower_bound']*self_row[input_row['column_name']]
                        upper_range=dynamic_range_vars[f'{range_column}_upper_bound']*self_row[input_row['column_name']]
                        temp=temp[(temp[range_column]>=lower_range) &(temp[range_column]<=upper_range) ]
                        temp.insert(1, f'self_{range_column}', self_row[range_column])
                        print(len(temp))
                    if float(input_row['range'])>=1:
                        greater_than_column=input_row['column_name']
                        temp=temp[temp[greater_than_column] > input_row['range']]
                        temp.insert(1, f'self_{greater_than_column}', self_row[greater_than_column])
                        print(len(temp))
                except:
                    to_insert=input_row['column_name']
                    range_bin_column=input_row['column_name']+'_bin'
                    temp=temp[temp[range_bin_column]==self_row[range_bin_column]]
                    temp.insert(1, f'self_{to_insert}', self_row[to_insert])
                    
                    print(len(temp))
        elif input_row['type'] == 'categorical':
            if input_row['range'] == '':
                
                categorical_exact_column=input_row['column_name']
                if self_row[categorical_exact_column] is not None:
                    temp=temp[(temp[categorical_exact_column] == self_row[categorical_exact_column])|(temp[categorical_exact_column] == None)]
                    temp.insert(1, f'self_{categorical_exact_column}', self_row[categorical_exact_column])
                    print(len(temp))
            else:
                categorical_list_column=input_row['column_name']
                if self_row[categorical_list_column] is not None:
                    temp=temp[temp[categorical_list_column].isin(dynamic_categorical_values[categorical_list_column])]
                    temp.insert(1, f'self_{categorical_list_column}', self_row[categorical_list_column])
                    print(len(temp))

        temp=temp[temp['Brand']!= self_row['Brand']]


    if not temp.empty:
        temp.insert(0, 'self_fsn', self_row['fsn'])
        temp.insert(1, 'self_price', self_row['final_selling_price'])
        temp.rename({"fsn": "competitor_fsn"}, axis=1, inplace=True)
        final_df_list.append(temp)

if final_df_list:
    final_df = pd.concat(final_df_list, ignore_index=True)
else:
    final_df = pd.DataFrame() 

# Convert all categorical columns to strings
final_df = final_df.astype({col: 'string' for col in final_df.select_dtypes(['category']).columns})

# Check if NaNs still exist and fill them with an empty string
final_df = final_df.fillna(' ')
try:
    final_df['Net Quantity']=final_df['Net Quantity'].astype(float)
    final_df['self_Total no of Pieces']=final_df['self_Total no of Pieces'].astype(float)
    final_df['per_unit_competitor_price']=final_df['final_selling_price']/final_df['Net Quantity']
    final_df['per_unit_self_price']=final_df['self_final_selling_price']/final_df['self_Total no of Pieces']
except:
    pass
# Now call your function to print to the sheet
general.add_sheet_name(output_sheet,'competitor output',1)
general.print_sheet(1, final_df, 'competitor output', output_sheet, 1, 1, 1)
self_data = self_data.astype({col: 'string' for col in self_data.select_dtypes(['category']).columns})
competitor_data = competitor_data.astype({col: 'string' for col in competitor_data.select_dtypes(['category']).columns})
# Check if NaNs still exist and fill them with an empty string
self_data = self_data.fillna(' ')
competitor_data = competitor_data.fillna(' ')
general.add_sheet_name(output_sheet,'self_dump',1)
general.print_sheet(1, self_data, 'self_dump', output_sheet, 1, 1, 1)
general.add_sheet_name(output_sheet,'competitor_dump',1)
general.print_sheet(1, competitor_data, 'competitor_dump', output_sheet, 1, 1, 1)