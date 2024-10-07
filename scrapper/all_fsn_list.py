import http.client
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
import threading
import pandas as pd
import gzip
import io

# Thread-local data to store per-thread HTTPSConnection
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
                print(f"Search '{search}' Page {page}: Found {len(data_list)} data-ids")
                all_data.extend(data_list)
            except Exception as exc:
                print(f"Search '{search}' Page {page} generated an exception: {exc}")

    return all_data