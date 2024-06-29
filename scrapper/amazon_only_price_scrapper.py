import pandas as pd
from fake_useragent import UserAgent
import requests
import json



def only_price_amazon_scrapper(asin_list):
    asin_prices = {}

    batch_size = 8

    for i in range(0, len(asin_list), batch_size):
        batch_asins = ','.join(asin_list[i:i+batch_size])

        url = f"https://www.amazon.in/gp/twister/dimension?isDimensionSlotsAjax=1&asinList={batch_asins}"

        payload = {}
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        data = response.text

        json_strings = data.split("&&&")

        for json_str in json_strings:
            try:
                json_data = json.loads(json_str.strip())
                asin = json_data.get("ASIN")
                price = json_data.get("Value", {}).get("content", {}).get("twisterSlotJson", {}).get("price")

                # Store ASIN and price in the dictionary
                asin_prices[asin] = price
            except json.JSONDecodeError as e:
                print("Error decoding JSON:", e)

    df = pd.DataFrame(list(asin_prices.items()), columns=['ASIN', 'Price'])

    try:
        df['Price'] = df['Price'].astype(float).astype(int)
    except:
        try:
            df['Price'] = df['Price'].astype(float)
        except:
            pass

    return df
