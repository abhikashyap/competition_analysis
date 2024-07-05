import json
import requests
import pandas as pd
import os
import pygsheets
import httpx
import concurrent.futures
from selectolax.parser import HTMLParser
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.chrome.options import Options


def collecting_Flipkart_Data(FSN):


    url = "https://1.rome.api.flipkart.com/api/4/page/fetch"

    payload = json.dumps({
        "pageUri": f"/a/p/a?pid={FSN}",
        "locationContext": {
            "pincode": "560066"
        },
        "isReloadRequest": True
    })

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9,en-IN;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Cookie': 'T=clpxn4xzy06470wd2ztjy5pj8-BR1702101250078; dpr=1; _pxvid=60779be7-9657-11ee-af89-338edb3a8d5b; vh=752; vw=1530; _gcl_au=1.1.1079217834.1702903922; _gid=GA1.2.6600805.1705499941; mp_9ea3bc9a23c575907407cf80efd56524_mixpanel=%7B%22distinct_id%22%3A%20%22ACC9644F91FB39448188106BA68C24709F8D%22%2C%22%24device_id%22%3A%20%2218d17b7d4db777-06bb98bdf4808a-17462c6f-15f900-18d17b7d4dc131c%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%2C%22%24user_id%22%3A%20%22ACC9644F91FB39448188106BA68C24709F8D%22%7D; AMCVS_55CFEDA0570C3FA17F000101%40AdobeOrg=1; AMCV_55CFEDA0570C3FA17F000101%40AdobeOrg=-227196251%7CMCIDTS%7C19740%7CMCMID%7C36873895514639056584390477893388312916%7CMCAAMLH-1706104756%7C12%7CMCAAMB-1706104756%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1705507156s%7CNONE%7CMCAID%7CNONE; _ga=GA1.2.1489252585.1702903922; _ga_TVF0VCMCT3=GS1.1.1705499940.6.1.1705499956.44.0.0; moe_uuid=990b4aa9-f52e-4592-9703-947b29f8da91; s_nr=1705500035468-Repeat; _ga_0SJLGHBL81=GS1.1.1705499940.6.1.1705500035.0.0.0; Network-Type=4g; AMCVS_17EB401053DAF4840A490D4C%40AdobeOrg=1; AMCV_17EB401053DAF4840A490D4C%40AdobeOrg=-227196251%7CMCIDTS%7C19741%7CMCMID%7C43990386728713327323661047710453072722%7CMCAAMLH-1706176097%7C12%7CMCAAMB-1706176097%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1705578497s%7CNONE%7CMCAID%7CNONE; pxcts=b4dffb16-b5e6-11ee-a796-bd365d618683; gpv_pn=HomePage; gpv_pn_t=FLIPKART%3AHomePage; S=d1t13P0sadT9JJUk/JQw/P1AiP7GEZg72uHn1BywMA2YYqzym/ipIjlSKvGXCMl2+ODGqUk3V1EktVIchHi3l8HcUow==; SN=VI8048300B964C487589C255C8EEDFE61D.TOK8534926BDA00472D8AF6CFA1D9EFC397.1705571443.LO; _px3=dc9ad67f3ac3f8530015aa57bcf00ca8ad654686b055df2e62665772dd21c810:N2lC4bvOxP1U39ED7T1OG83imRzCF61gigkr3yAb9cq3PcTni0XzA3GIARmuZ3dL1wz5EeyBWxO5wcRGa+bWyA==:1000:urP/XRgIzylJ912q+GaBtEKufjbtpLRXGUGJ36h49BjraJTL9mbOir+AkO/DH2hy6s5fO97Wg0d9HNLgIXdZPmP79isB+gBjpuFMST5isimWGxNPlQ59JyTbAn3W17gTH24Z3q+p/Hh7mYLMDQLn2507T8KbNLOwU9M6LwuS9S4GIk30DhaYVrrw5G/edMaT3+Uy5pagpr+6aH2n61rEpo7h4Kg+XYkVsG55qtNyxU4=; s_sq=flipkart-prd%3D%2526pid%253Dwww.flipkart.com%25253Abraun-face-mini-hair-remover-fs1000-cordless-epilator-women%25253Ap%25253Aitm3b6779c5ca9be%2526pidt%253D1%2526oid%253DfunctionSr%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DLI%26flipkartsellerprod%3D%2526c.%2526a.%2526activitymap.%2526page%253Dhttps%25253A%25252F%25252Fseller.flipkart.com%25252Findex.html%252523dashboard%25252Flistings-management%25253FlistingState%25253DACTIVE%2526link%253DDownload%2526region%253Dbulk-file-history%2526.activitymap%2526.a%2526.c%2526pid%253Dhttps%25253A%25252F%25252Fseller.flipkart.com%25252Findex.html%252523dashboard%25252Flistings-management%25253FlistingState%25253DACTIVE%2526oid%253Dhttps%25253A%25252F%25252Fseller.flipkart.com%25252Fnapi%25252Flisting%25252FstockFileDownload%25253FsellerId%25253D948d1f55eead4eaa%252526fileId%25253D%2525252Fopera%2526ot%253DA',
        'Origin': 'https://www.flipkart.com',
        'Referer': 'https://www.flipkart.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'X-User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 FKUA/website/42/website/Desktop',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Brave";v="126"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"'
    }

    try:
        response = httpx.post(url, headers=headers, content=payload)
        response.raise_for_status()  # Raises HTTPStatusError for bad responses (4xx and 5xx)
        # print(f"Response status code: {response.status_code}")
        # print(f"Response headers: {response.headers}")

    # except httpx.HTTPStatusError as http_err:
    #     print(f"HTTP error occurred: {http_err}")  # Python 3.6
    #     print(f"Response status code: {response.status_code}")

    except Exception as err:
        pass
        # print(f"Other error occurred: {err}")  # Python 3.6
    data = json.loads(response.text)
    return data

# def fk_other_seller_info_api(fsn):
#     url = "https://2.rome.api.flipkart.com/api/3/page/dynamic/product-sellers"

#     payload = json.dumps({
#       "requestContext": {
#         "productId": fsn
#       },
#       "locationContext": {}
#     })
#     headers = {
#       'Content-Type': 'application/json',
#       'Accept-Language': 'en-US,en;q=0.9,en-IN;q=0.8',
#       'Content-Type': 'application/json',
#       'Cookie': 'T=SD.7673f33b-ed73-4c2d-9c41-0f3bfff898c0.1697997323823; _gcl_au=1.1.609917794.1697997325; at=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjFkOTYzYzUwLTM0YjctNDA1OC1iMTNmLWY2NDhiODFjYTBkYSJ9.eyJleHAiOjE2OTk3ODU4OTEsImlhdCI6MTY5ODA1Nzg5MSwiaXNzIjoia2V2bGFyIiwianRpIjoiZGViYTk5NTItZWYwZC00OGYyLTg2MTgtZTdhZThiMmE4ZjRjIiwidHlwZSI6IkFUIiwiZElkIjoiU0QuNzY3M2YzM2ItZWQ3My00YzJkLTljNDEtMGYzYmZmZjg5OGMwLjE2OTc5OTczMjM4MjMiLCJrZXZJZCI6IlZJODczMTU5MUI3QkZENDYwQ0FCQzQwMkU2MEM2MUIxRkMiLCJ0SWQiOiJtYXBpIiwidnMiOiJMTyIsInoiOiJIWUQiLCJtIjp0cnVlLCJnZW4iOjR9.vZinRn_V98wzIIZ4cINaJoVsHETxMdUO-GdoVaDLEk4; K-ACTION=null; vh=944; vw=1920; dpr=1; _pxvid=33951c97-7191-11ee-8c84-54525af8b9ad; AMCV_55CFEDA0570C3FA17F000101%40AdobeOrg=-227196251%7CMCIDTS%7C19659%7CMCMID%7C75907747707542419911271218439759702261%7CMCAID%7CNONE; _gid=GA1.2.1508938059.1698474410; _ga=GA1.1.7334968.1697997325; _ga_0SJLGHBL81=GS1.1.1698474410.3.1.1698474715.0.0.0; _ga_TVF0VCMCT3=GS1.1.1698474410.3.1.1698474715.60.0.0; pxcts=b679308e-755b-11ee-8b89-966d8cf69a7a; AMCVS_17EB401053DAF4840A490D4C%40AdobeOrg=1; AMCV_17EB401053DAF4840A490D4C%40AdobeOrg=-227196251%7CMCIDTS%7C19659%7CMCMID%7C69790193157414311032196335614131668850%7CMCAAMLH-1698662694%7C12%7CMCAAMB-1699079526%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1698481926s%7CNONE%7CMCAID%7CNONE; Network-Type=4g; s_sq=flipkart-prd%3D%2526pid%253Dwww.flipkart.com%25253Amotorola-edge-40-nebula-green-256-gb%25253Ap%25253Aitmaa0bf9c327a2b%2526pidt%253D1%2526oid%253DfunctionSr%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DSUBMIT; S=d1t11aD9zPwxAFz9LPRJ/V14/P0IcH6KMbBhSv5LFVD7NEPZfY9g3ewjTuLhgNWqgPJnxkMfu/+IjFQ2gVVVepRvUog==; _px3=d84a84171eff186b6087b179779f570bcdb40f8c77b17ebed345f601b4d90a47:ByOdQmYADre9+tzJwqnbkP+HBnPq45pAPiRshP9FI9tl2LznP9wRPAe/DdIcQobc/b3UZELaQ69UgzHtQfZTvA==:1000:zIQq5kOkzAHQEa8e7iIulRqUmCwpyOCuaUGRsvGK1nFK0bVooYQmstLf6StA6Knar6xlfPHRN0ceDnf2f6zZD0q3IvOAKRXorElY+a00/PKtOvd56DRXe5h6SPDobHY9utyDSu6YiIWXcJDqtAnUXVlrfC+mPgMwCoijl12o7Pa3+257Zwt32Fq02ucNLFTLtZYIu5IN89Hdm3nxR+BFyommPJ2Hc50omHEQJEZYN4w=; SN=VI8731591B7BFD460CABC402E60C61B1FC.TOKEDDE7E8A0D334914AADAC4783F123602.1698482505.LO',
#       'Origin': 'https://www.flipkart.com',
#       'Referer': 'https://www.flipkart.com/',
#       'Sec-Fetch-Dest': 'empty',
#       'Sec-Fetch-Mode': 'cors',
#       'Sec-Fetch-Site': 'same-site',
#       'X-User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 FKUA/website/42/website/Desktop',
#       'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
#       'sec-ch-ua-mobile': '?0',
#       'sec-ch-ua-platform': '"Linux"'
#     }

#     response = requests.request("POST", url, headers=headers, data=payload)

#     data=json.loads(response.text)
#     return data
# def fk_other_seller_info_api(fsn):
#     url = "https://2.rome.api.flipkart.com/api/3/page/dynamic/product-sellers"

#     payload = json.dumps({
#         "requestContext": {
#             "productId": fsn
#         },
#         "locationContext": {}
#     })
#     headers = {
#         'Content-Type': 'application/json',
#         'Accept-Language': 'en-US,en;q=0.9,en-IN;q=0.8',
#         'Cookie': 'T=SD.7673f33b-ed73-4c2d-9c41-0f3bfff898c0.1697997323823; _gcl_au=1.1.609917794.1697997325; at=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjFkOTYzYzUwLTM0YjctNDA1OC1iMTNmLWY2NDhiODFjYTBkYSJ9.eyJleHAiOjE2OTk3ODU4OTEsImlhdCI6MTY5ODA1Nzg5MSwiaXNzIjoia2V2bGFyIiwianRpIjoiZGViYTk5NTItZWYwZC00OGYyLTg2MTgtZTdhZThiMmE4ZjRjIiwidHlwZSI6IkFUIiwiZElkIjoiU0QuNzY3M2YzM2ItZWQ3My00YzJkLTljNDEtMGYzYmZmZjg5OGMwLjE2OTc5OTczMjM4MjMiLCJrZXZJZCI6IlZJODczMTU5MUI3QkZENDYwQ0FCQzQwMkU2MEM2MUIxRkMiLCJ0SWQiOiJtYXBpIiwidnMiOiJMTyIsInoiOiJIWUQiLCJtIjp0cnVlLCJnZW4iOjR9.vZinRn_V98wzIIZ4cINaJoVsHETxMdUO-GdoVaDLEk4; K-ACTION=null; vh=944; vw=1920; dpr=1; _pxvid=33951c97-7191-11ee-8c84-54525af8b9ad; AMCV_55CFEDA0570C3FA17F000101%40AdobeOrg=-227196251%7CMCIDTS%7C19659%7CMCMID%7C75907747707542419911271218439759702261%7CMCAID%7CNONE; _gid=GA1.2.1508938059.1698474410; _ga=GA1.1.7334968.1697997325; _ga_0SJLGHBL81=GS1.1.1698474410.3.1.1698474715.0.0.0; _ga_TVF0VCMCT3=GS1.1.1698474410.3.1.1698474715.60.0.0; pxcts=b679308e-755b-11ee-8b89-966d8cf69a7a; AMCVS_17EB401053DAF4840A490D4C%40AdobeOrg=1; AMCV_17EB401053DAF4840A490D4C%40AdobeOrg=-227196251%7CMCIDTS%7C19659%7CMCMID%7C69790193157414311032196335614131668850%7CMCAAMLH-1698662694%7C12%7CMCAAMB-1699079526%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1698481926s%7CNONE%7CMCAID%7CNONE; Network-Type=4g; s_sq=flipkart-prd%3D%2526pid%253Dwww.flipkart.com%25253Amotorola-edge-40-nebula-green-256-gb%25253Ap%25253Aitmaa0bf9c327a2b%2526pidt%253D1%2526oid%253DfunctionSr%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DSUBMIT; S=d1t11aD9zPwxAFz9LPRJ/V14/P0IcH6KMbBhSv5LFVD7NEPZfY9g3ewjTuLhgNWqgPJnxkMfu/+IjFQ2gVVVepRvUog==; _px3=d84a84171eff186b6087b179779f570bcdb40f8c77b17ebed345f601b4d90a47:ByOdQmYADre9+tzJwqnbkP+HBnPq45pAPiRshP9FI9tl2LznP9wRPAe/DdIcQobc/b3UZELaQ69UgzHtQfZTvA==:1000:zIQq5kOkzAHQEa8e7iIulRqUmCwpyOCuaUGRsvGK1nFK0bVooYQmstLf6StA6Knar6xlfPHRN0ceDnf2f6zZD0q3IvOAKRXorElY+a00/PKtOvd56DRXe5h6SPDobHY9utyDSu6YiIWXcJDqtAnUXVlrfC+mPgMwCoijl12o7Pa3+257Zwt32Fq02ucNLFTLtZYIu5IN89Hdm3nxR+BFyommPJ2Hc50omHEQJEZYN4w=; SN=VI8731591B7BFD460CABC402E60C61B1FC.TOKEDDE7E8A0D334914AADAC4783F123602.1698482505.LO',
#         'Origin': 'https://www.flipkart.com',
#         'Referer': 'https://www.flipkart.com/',
#         'Sec-Fetch-Dest': 'empty',
#         'Sec-Fetch-Mode': 'cors',
#         'Sec-Fetch-Site': 'same-site',
#         'X-User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 FKUA/website/42/website/Desktop',
#         'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
#         'sec-ch-ua-mobile': '?0',
#         'sec-ch-ua-platform': '"Linux"'
#     }

#     try:
#         response = httpx.post(url, headers=headers, content=payload)
#         response.raise_for_status()  # Raises HTTPStatusError for bad responses (4xx and 5xx)
#         # data = response.json()
#         # return data
#     except httpx.HTTPStatusError as http_err:
#         print(f"HTTP error occurred: {http_err}")  # Python 3.6
#         print(f"Response status code: {response.status_code}")
#         # print(f"Response body: {response.text}")
#     except Exception as err:
#         print(f"Other error occurred: {err}")  # Python 3.6
#     data=json.loads(response.text)
#     return data

def title(data):
    title = ""
    try:
        
        if 'subtitle' in data['RESPONSE']['pageData']['pageContext']['titles']:
            title = data['RESPONSE']['pageData']['pageContext']['seo']['title'] + \
                ' ' + data['RESPONSE']['pageData']['pageContext']['titles']['subtitle']
        else:
            title = data['RESPONSE']['pageData']['pageContext']['seo']['title']
    except:
        pass
    return title


def rating(data):
    rating = ""
    try:
        
        if 'pageData' in data['RESPONSE']:
            if 'pageContext' in data['RESPONSE']['pageData']:
                if 'fdpEventTracking' in data['RESPONSE']['pageData']['pageContext']:
                    if data['RESPONSE']['pageData']['pageContext']['fdpEventTracking'] != None:
                        if 'events' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']:
                            if 'psi' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']:
                                if 'pr' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']:
                                    if 'rating' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']['pr']:
                                        rating = data['RESPONSE']['pageData']['pageContext'][
                                            'fdpEventTracking']['events']['psi']['pr']['rating']

    except:
        pass
    return rating


def ratings_count(data):
    ratings_count = ""
    try:
        if 'pageData' in data['RESPONSE']:
            if 'pageContext' in data['RESPONSE']['pageData']:
                if 'fdpEventTracking' in data['RESPONSE']['pageData']['pageContext']:
                    if 'events' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']:
                        if 'psi' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']:
                            if 'pr' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']:
                                if 'ratingsCount' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']['pr']:
                                    ratings_count = data['RESPONSE']['pageData']['pageContext'][
                                        'fdpEventTracking']['events']['psi']['pr']['ratingsCount']
    except:
        pass
    return ratings_count


def reviews_count(data):
    reviews_count = ""
    try:
        
        if 'pageData' in data['RESPONSE']:
            if 'pageContext' in data['RESPONSE']['pageData']:
                if 'fdpEventTracking' in data['RESPONSE']['pageData']['pageContext']:
                    if 'events' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']:
                        if 'psi' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']:
                            if 'pr' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']:
                                if 'reviewsCount' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']['pr']:
                                    reviews_count = data['RESPONSE']['pageData']['pageContext'][
                                        'fdpEventTracking']['events']['psi']['pr']['reviewsCount']
    except:
        pass
    return reviews_count


def special_price(data):
    special_price = ""
    try:
        
        special_price = data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']['ppd']['isSpecialPrice']
    except:
        pass
    return special_price


def final_selling_price(data):
    final_selling_price = ""
    try:
        
        if 'pageData' in data['RESPONSE']:
            if 'pageContext' in data['RESPONSE']['pageData']:
                if 'fdpEventTracking' in data['RESPONSE']['pageData']['pageContext']:
                    if 'events' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']:
                        if 'psi' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']:
                            if 'ppd' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']:
                                if 'finalPrice' in data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']['ppd']:
                                    final_selling_price = data['RESPONSE']['pageData']['pageContext'][
                                        'fdpEventTracking']['events']['psi']['ppd']['finalPrice']
    except:
        pass
    return final_selling_price


def mrp(data):
    mrp = ""
    try:
        
        mrp = data['RESPONSE']['pageData']['pageContext']['fdpEventTracking']['events']['psi']['ppd']['mrp']
    except:
        pass
    return mrp


def highlights(data):
    highlights = ""
    try:
        
        slot_data = data['RESPONSE']['slots']
        for each_slot in slot_data:
            if 'widget' in each_slot:
                if 'data' in each_slot['widget']:
                    if 'highlights' in each_slot['widget']['data']:
                        if 'value' in each_slot['widget']['data']['highlights']:
                            if 'text' in each_slot['widget']['data']['highlights']['value']:
                                highlights = each_slot['widget']['data']['highlights']['value']['text']
    except:
        pass
    return highlights


def paymentOptions(data):
    paymentoptions = ""
    try:
        
        slot_data = data['RESPONSE']['slots']
        for each_slot in slot_data:
            if 'widget' in each_slot:
                if 'data' in each_slot['widget']:
                    if 'paymentOptions' in each_slot['widget']['data']:
                        for each_option in each_slot['widget']['data']['paymentOptions']:
                            if 'value' in each_option:
                                paymentoptions = each_option['value']['text']
    except:
        pass
    return paymentoptions


def description(data):
    description = ""
    try:
        
        slot_data = data['RESPONSE']['slots']
        for each_slot in slot_data:
            if 'widget' in each_slot:
                if 'data' in each_slot['widget']:
                    if 'renderableComponents' in each_slot['widget']['data']:
                        if each_slot['widget']['data']['renderableComponents'] != None:
                            for component in each_slot['widget']['data']['renderableComponents']:
                                if 'value' in component:
                                    if 'text' in component['value']:
                                        description = component['value']['text']
    except:
        pass
    return description


def productDescription(data):
    
    try:
        slot_data = data['RESPONSE']['slots']
        d = {}
        product_description_title = []
        product_description = []
        for each_slot in slot_data:
            if 'widget' in each_slot:
                if 'data' in each_slot['widget']:
                    if 'featureSetList' in each_slot['widget']['data']:
                        for each_feature in each_slot['widget']['data']['featureSetList']:
                            if 'features' in each_feature:
                                for each_description in each_feature['features']:
                                    if 'title' in each_description:
                                        product_description_title.append(
                                            each_description['title'])
                                    if 'text' in each_description['description']:
                                        product_description.append(
                                            each_description['description']['text'])
        product = []
        for title, description in zip(product_description_title, product_description):
            product.append(title+' : '+description)
    except:
        pass
    return product


def specifications(data):
    try:
        count = 0
        keys = []
        values = []
        d = {}
        specifications_dict = {}
        slot_data = data['RESPONSE']['slots']
        for each_slot in slot_data:
            if 'widget' in each_slot:
                if 'data' in each_slot['widget']:
                    if 'renderableComponents' in each_slot['widget']['data']:
                        if each_slot['widget']['data']['renderableComponents'] != None:
                            for component in each_slot['widget']['data']['renderableComponents']:
                                if 'value' in component:
                                    if 'key' in component['value']:
                                        keys.append(component['value']['key'])
                                    if 'attributes' in component['value']:
                                        for each_attribute in component['value']['attributes']:
                                            d[each_attribute['name']] = each_attribute['values']
                                            count += 1
                                            if count == len(component['value']['attributes']):
                                                values.append(d)
                                                d = {}
                                                count = 0
        for dict in values:
            for key, value in dict.items():
                specifications_dict[key] = value[0]
    except:
        pass
    return specifications_dict

def colours(data):
    color_count=0
    try:
        response = data['RESPONSE']
        if 'slots' in response:
            slots = response['slots']
            for slot in slots:
                if 'widget' in slot:
                    widget = slot['widget']
                    if 'data' in widget:
                        data = widget['data']
                        if 'swatchComponent' in data:
                            swatchComponent = data['swatchComponent']
                            if 'value' in swatchComponent:
                                value = swatchComponent['value']
                                if 'attributeOptions' in value:
                                    attributeOptions = value['attributeOptions']
                                    color_count = len(attributeOptions[0])
                                    
    except:
        pass
    return color_count

# def size_count(data):
#     try:
#         response = data['RESPONSE']
#         if 'slots' in response:
#             slots = response['slots']
#             for slot in slots:
#                 if 'widget' in slot:
#                     widget = slot['widget']
#                     if 'data' in widget:
#                         data = widget['data']
#                         if 'swatchComponent' in data:
#                             swatchComponent = data['swatchComponent']
#                             if 'value' in swatchComponent:
#                                 value = swatchComponent['value']
#         return len(value['products'])
#     except:
#         pass

def reviews(data):
    l = []
    try:
        d = {}
        slot_data = data['RESPONSE']['slots']
        for each_slot in slot_data:
            if 'widget' in each_slot:
                if 'data' in each_slot['widget']:
                    if 'reviewData' in each_slot['widget']['data']:
                        if each_slot['widget']['data']['reviewData'] != None:
                            if 'renderableComponents' in each_slot['widget']['data']['reviewData']:
                                for component in each_slot['widget']['data']['reviewData']['renderableComponents']:
                                    if 'value' in component:
                                        if 'text' and 'title' and 'rating' in component['value']:
                                            d['rating'] = component['value']['rating']
                                            d['review_header'] = component['value']['title']
                                            d['review_content'] = component['value']['text']
                                            l.append(d)
    except:
        pass
    return l


def productImagesCount(data):
    product_images_count = ""
    try:
        
        if 'pageData' in data['RESPONSE']:
            if 'pageContext' in data['RESPONSE']['pageData']:
                if 'productImagesCount' in data['RESPONSE']['pageData']['pageContext']:
                    product_images_count = data['RESPONSE']['pageData']['pageContext']['productImagesCount']
        
    except:
        pass
    return product_images_count


def productVideosCount(data):
    product_video_count=0
    try:
        
        if 'pageData' in data['RESPONSE']:
            if 'pageContext' in data['RESPONSE']['pageData']:
                if 'productVideosCount' in data['RESPONSE']['pageData']['pageContext']:
                    product_video_count = data['RESPONSE']['pageData']['pageContext']['productVideosCount']
                    if product_video_count==None:
                        return 0
                    else:
                        return product_video_count
    except:
        pass
    return product_video_count

# def other_seller_info(fsn):
#     api_data=fk_other_seller_info_api(fsn)
#     api_details = {}
#     try:
#         no_of_other_seller = len(api_data['RESPONSE']['data']['product_seller_detail_1']['data'])
#         for i in range(no_of_other_seller):
#             key_api = api_data['RESPONSE']['data']['product_seller_detail_1']['data'][i]['value']['sellerInfo']['value']['name']
#             value_api= api_data['RESPONSE']['data']['product_seller_detail_1']['data'][i]['value']['metadata']['price']
#             api_details[key_api] = value_api
#     except:
#         pass
#     return api_details
def flipkartAssured(data):
    flipkart_assured=''
    try:
        
        slot_data = data['RESPONSE']['slots']
        for each_slot in slot_data:
            if 'WIDGET' in each_slot['slotType']:
                if 'data' in (each_slot['widget'].keys()):
                    if 'deliveryData' in (each_slot['widget']['data']):
                        if 'deliveryMeta' in(each_slot['widget']['data']['deliveryData']):
                            flipkart_assured=(each_slot['widget']['data']['deliveryData']['deliveryMeta']['fAssured'])
    except:
        pass
    return flipkart_assured
def sellerName(data):
    seller_name=''
    try:
        
        if 'pageContext' in data['RESPONSE']['pageData']:
            if 'trackingDataV2' in (data['RESPONSE']['pageData']['pageContext']):
                seller_name=data['RESPONSE']['pageData']['pageContext']['trackingDataV2']['sellerName']
    except:
        pass
    return seller_name

def image_url(data):
    imageurls_path = [data.get('RESPONSE', {}).get('pageData', {}).get('pageContext', {}).get('imageUrl', {}),
         data.get('image_url', {}),
    ]
    for url in imageurls_path:
        if url!={}:
            imageurl = url
            imageurl = imageurl.replace('{@width}', '832').replace('{@height}','832').replace('{@quality}', '70')
            if imageurl == {}:
                imageurl = None
            return imageurl

def all_specifications(data):
    slots = data["RESPONSE"]["slots"]
    for index in range(len(slots)):
        key_dict = {}
        try:
            renderable_components = data["RESPONSE"]["slots"][index]["widget"]["data"]["renderableComponents"]
            for component in range(len(renderable_components)):
                specification_dict = {}
                specifications = data["RESPONSE"]["slots"][index]["widget"]["data"]["renderableComponents"][component]["value"]["attributes"]
                for each_dict in specifications:
                    values = list(each_dict.values())
                    specification_dict[values[0]] = values[-1][0]
                key = data["RESPONSE"]["slots"][index]["widget"]["data"]["renderableComponents"][component]["value"]["key"]
                key_dict[key] = specification_dict
            k = {}
            for key, value in key_dict.items():
                k.update(value)
            break
        except:
            pass
    return k

def all_specs(data):
    slots = data["RESPONSE"]["slots"]
    for index in range(len(slots)):
        try:
            specifications = data["RESPONSE"]["slots"][index]["widget"]["data"]["renderableComponent"]["value"]["specification"]
            break
        except:
            pass
    specification_dict = {}
    for each_dict in specifications:
        values = list(each_dict.values())
        specification_dict[values[0]] = values[-1][0]
    return specification_dict

def make_a_request(fsn, max_retries=3):
    for _ in range(max_retries):
        try:
            d = {}
            info = collecting_Flipkart_Data(fsn)
            d['fsn'] = fsn
            d['title'] = title(info)
            d['rating'] = rating(info)
            d['ratings_count'] = ratings_count(info)
            d['reviews_count'] = reviews_count(info)
            d['special_price'] = special_price(info)
            d['final_selling_price'] = final_selling_price(info)
            d['mrp'] = mrp(info)
            d['flipkart_assured'] = flipkartAssured(info)
            d['Seller Name'] =sellerName(info)
            # d['colours'] = colours(info)
            d['highlights'] = highlights(info)
            d['paymentOptions'] = paymentOptions(info)
            d['description'] = description(info)
            d['productDescription'] = productDescription(info)
            d['specifications'] = specifications(info)
            # d['fk_other_seller_info'] = other_seller_info(fsn)
            d['reviews'] = reviews(info)
            d['productImagesCount'] = productImagesCount(info)
            d['productVideosCount'] = productVideosCount(info)
            d['image_link'] = image_url(info)
            try:
                d['all_specs'] = all_specs(info)
            except:
                pass
            try:
                d["all_specs"] = all_specifications(info)
            except:
                pass
            # d['data']=info
            return d
        except httpx.HTTPError as e:

            if _ == max_retries - 1:
                raise
            else:
                continue  # Try again

def scrape_all_fsns(fsns, max_retries=3):
    scraped_data = []
    failed_fsns = []
    # for fsn in fsns:
    #     data=make_a_request(fsn)
    #     print(fsn,data)
    #     scraped_data.append(data)
    max_concurrent_requests = 50

    # Create a ThreadPoolExecutor to parallelize the scraping
    with concurrent.futures.ThreadPoolExecutor(max_concurrent_requests) as executor:
        future_to_fsn = {executor.submit(
            make_a_request, fsn, max_retries): fsn for fsn in fsns}

        for future in concurrent.futures.as_completed(future_to_fsn):
            fsn = future_to_fsn[future]
            try:
                data = future.result()  # Get the scraped data for the FSN
                if data not in scraped_data:
                    scraped_data.append(data)
            except Exception as e:
                # print(fsn)
                # print(str(e))

                failed_fsns.append(fsn)
    # for fsn in failed_fsns:
    #     try:
    #         additional_data = make_a_request(fsn, max_retries)
    #         scraped_data.append(additional_data)
    #     except Exception as e:
    #         print(f"Error for FSN {fsn}: {str(e)}")
    scraped_data=[one_dict for one_dict in scraped_data if len(one_dict) > 0]
    df = pd.DataFrame(scraped_data)
    # fk_scrapper.flipkart_scraping_data(df)
    return df

