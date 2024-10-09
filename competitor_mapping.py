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
# Thread-local data to store per-thread HTTPSConnection
thread_local = threading.local()
output_sheet='1vBoQA3yxu6glukkFO5ohaYgQeIQ5X3TAVY-dm9BwM0o'
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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/85.0.4183.102 Safari/537.36',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
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


search_term=general.read_sheet(output_sheet,"search",1)
search_queries=search_term['search_term'].to_list()
start_page = 1
end_page = 25

all_data = collect_all_data_ids(search_queries, start_page, end_page)
print(f"Total data-id attributes found: {len(all_data)}")

# Create DataFrame
search_term_page = pd.DataFrame(all_data, columns=['search_query', 'position', 'page_no', 'data_id'])

general.print_sheet(1,search_term_page,'search_output',output_sheet,1,1,1)


all_fsns = search_term_page['data_id'].to_list()
final_fsn_list = list(set(all_fsns))
competitor_data=fk_scrapper.scrape_all_fsns(final_fsn_list)
competitor_data['brand'] = competitor_data['title'].str.split(" ").str[0]

self=general.read_sheet(output_sheet,sheet_name='self')
self_fsn=self['fsn'].to_list()
self_data=fk_scrapper.scrape_all_fsns(self_fsn)


def convert_to_all_columns(brand_level_data):
    for index, row in brand_level_data.iterrows():
        specs = row['all_specs']

        if not isinstance(specs, dict):
            try:
                # Attempt to convert if specs is a string representation of a dictionary
                specs = ast.literal_eval(specs)
            except (ValueError, SyntaxError):
                # If conversion fails, skip processing this row
                continue

        # Process dictionary and update the DataFrame
        if isinstance(specs, dict):
            for key, value in specs.items():
                if key not in brand_level_data.columns:
                    brand_level_data[key] = None  # Add new column if it doesn't exist
                brand_level_data.at[index, key] = value

    return brand_level_data
competitor_data=convert_to_all_columns(competitor_data)
self_data=convert_to_all_columns(self_data)
competitor_data=competitor_data[competitor_data['ratings_count'] != ""]


# Create an empty dictionary to store column statistics
d = {}

# Calculate unique values, percentage of non-null values, and the unique value list for each column
for column in competitor_data.columns:
    try:
        unique_count = competitor_data[column].nunique()
        not_na_percentage = competitor_data[column].notna().mean() * 100
        unique_values_list = competitor_data[column].unique().tolist()  # Get the list of unique values
        d[column] = [unique_count, not_na_percentage, unique_values_list]

    except:
        # Handle errors by converting the column to string type
        competitor_data[column] = competitor_data[column].astype(str)
        unique_count = competitor_data[column].nunique()
        not_na_percentage = competitor_data[column].notna().mean() * 100
        unique_values_list = competitor_data[column].unique().tolist()  # Get the list of unique values
        d[column] = [unique_count, not_na_percentage, unique_values_list]

# Convert the dictionary into a DataFrame
column_stats_df = pd.DataFrame.from_dict(d, orient='index', columns=['Unique Values', '% Not NaN', 'Unique Values List'])
column_stats_df.reset_index(inplace=True)
column_stats_df.rename(columns={'index': 'Column Name'}, inplace=True)

df=column_stats_df.sort_values("% Not NaN",ascending=False)
not_to_consider_columns=['productDescription','productImagesCount','productVideosCount','Country of Origin','flipkart_assured','special_price',
                         'title','rating','ratings_count','reviews_count','Seller Name','highlights','description','specifications','reviews',
                         'image_link','all_specs','brand','Model Name','mrp']
df=df[(df['Unique Values'] != df['Unique Values'].max())& (df['Unique Values'] !=1)& (~df['Column Name'].isin(not_to_consider_columns))]
general.print_sheet(1,df,'research_of_attributes',output_sheet,1,1,1)
input_df=general.read_sheet(output_sheet,'attribute',1)

def extract_number(text):
    numbers=re.findall(r'\d+',str(text))
    if len(numbers) != 0:
        ans=max(numbers)
    else:
        ans=0
    return ans

# function is created to format data 
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
general.print_sheet(1, final_df, 'competitor output', output_sheet, 1, 1, 1)