import http.client
from urllib.parse import urlencode
from bs4 import BeautifulSoup
def get_fsn(search,page):
    conn = http.client.HTTPSConnection("www.flipkart.com")
    payload = ''
    headers = {}
    params = {
    'q': search,
    'page': '2'
    }
    query_string = urlencode(params)

    conn.request("GET", f"/search?{query_string}&page={page}", payload, headers)
    res = conn.getresponse()
    data = res.read()

    soup = BeautifulSoup(data, 'html.parser')

    elements_with_data_id = soup.find_all(attrs={'data-id': True})

    # Extract and print the 'data-id' attribute from each element
    l=[]
    for element in elements_with_data_id:
        data_id = element['data-id']
        l.append(data_id)

    return l
get_fsn(search,page)