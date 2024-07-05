# import requests
# import json
# FSN='MOBGT4RZUPKD8HDK'
# url = "https://1.rome.api.flipkart.com/api/4/page/fetch"

# payload = json.dumps({
# "pageUri": f"/a/p/a?pid={FSN}",
# "locationContext": {
#     "pincode": "560066"
# },
# "isReloadRequest": True
# })
# headers = {
# # 'Accept': '*/*',
# # 'Accept-Language': 'en-US,en;q=0.9,en-IN;q=0.8',
# # 'Connection': 'keep-alive',
# # 'Content-Type': 'application/json',
# # 'Cookie': 'T=clpxn4xzy06470wd2ztjy5pj8-BR1702101250078; dpr=1; _pxvid=60779be7-9657-11ee-af89-338edb3a8d5b; vh=752; vw=1530; _gcl_au=1.1.1079217834.1702903922; _gid=GA1.2.6600805.1705499941; mp_9ea3bc9a23c575907407cf80efd56524_mixpanel=%7B%22distinct_id%22%3A%20%22ACC9644F91FB39448188106BA68C24709F8D%22%2C%22%24device_id%22%3A%20%2218d17b7d4db777-06bb98bdf4808a-17462c6f-15f900-18d17b7d4dc131c%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%2C%22%24user_id%22%3A%20%22ACC9644F91FB39448188106BA68C24709F8D%22%7D; AMCVS_55CFEDA0570C3FA17F000101%40AdobeOrg=1; AMCV_55CFEDA0570C3FA17F000101%40AdobeOrg=-227196251%7CMCIDTS%7C19740%7CMCMID%7C36873895514639056584390477893388312916%7CMCAAMLH-1706104756%7C12%7CMCAAMB-1706104756%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1705507156s%7CNONE%7CMCAID%7CNONE; _ga=GA1.2.1489252585.1702903922; _ga_TVF0VCMCT3=GS1.1.1705499940.6.1.1705499956.44.0.0; moe_uuid=990b4aa9-f52e-4592-9703-947b29f8da91; s_nr=1705500035468-Repeat; _ga_0SJLGHBL81=GS1.1.1705499940.6.1.1705500035.0.0.0; Network-Type=4g; AMCVS_17EB401053DAF4840A490D4C%40AdobeOrg=1; AMCV_17EB401053DAF4840A490D4C%40AdobeOrg=-227196251%7CMCIDTS%7C19741%7CMCMID%7C43990386728713327323661047710453072722%7CMCAAMLH-1706176097%7C12%7CMCAAMB-1706176097%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1705578497s%7CNONE%7CMCAID%7CNONE; pxcts=b4dffb16-b5e6-11ee-a796-bd365d618683; gpv_pn=HomePage; gpv_pn_t=FLIPKART%3AHomePage; S=d1t13P0sadT9JJUk/JQw/P1AiP7GEZg72uHn1BywMA2YYqzym/ipIjlSKvGXCMl2+ODGqUk3V1EktVIchHi3l8HcUow==; SN=VI8048300B964C487589C255C8EEDFE61D.TOK8534926BDA00472D8AF6CFA1D9EFC397.1705571443.LO; _px3=dc9ad67f3ac3f8530015aa57bcf00ca8ad654686b055df2e62665772dd21c810:N2lC4bvOxP1U39ED7T1OG83imRzCF61gigkr3yAb9cq3PcTni0XzA3GIARmuZ3dL1wz5EeyBWxO5wcRGa+bWyA==:1000:urP/XRgIzylJ912q+GaBtEKufjbtpLRXGUGJ36h49BjraJTL9mbOir+AkO/DH2hy6s5fO97Wg0d9HNLgIXdZPmP79isB+gBjpuFMST5isimWGxNPlQ59JyTbAn3W17gTH24Z3q+p/Hh7mYLMDQLn2507T8KbNLOwU9M6LwuS9S4GIk30DhaYVrrw5G/edMaT3+Uy5pagpr+6aH2n61rEpo7h4Kg+XYkVsG55qtNyxU4=; s_sq=flipkart-prd%3D%2526pid%253Dwww.flipkart.com%25253Abraun-face-mini-hair-remover-fs1000-cordless-epilator-women%25253Ap%25253Aitm3b6779c5ca9be%2526pidt%253D1%2526oid%253DfunctionSr%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DLI%26flipkartsellerprod%3D%2526c.%2526a.%2526activitymap.%2526page%253Dhttps%25253A%25252F%25252Fseller.flipkart.com%25252Findex.html%252523dashboard%25252Flistings-management%25253FlistingState%25253DACTIVE%2526link%253DDownload%2526region%253Dbulk-file-history%2526.activitymap%2526.a%2526.c%2526pid%253Dhttps%25253A%25252F%25252Fseller.flipkart.com%25252Findex.html%252523dashboard%25252Flistings-management%25253FlistingState%25253DACTIVE%2526oid%253Dhttps%25253A%25252F%25252Fseller.flipkart.com%25252Fnapi%25252Flisting%25252FstockFileDownload%25253FsellerId%25253D948d1f55eead4eaa%252526fileId%25253D%2525252Fopera%2526ot%253DA',
# # 'Origin': 'https://www.flipkart.com',
# # 'Referer': 'https://www.flipkart.com/',
# # 'Sec-Fetch-Dest': 'empty',
# # 'Sec-Fetch-Mode': 'cors',
# # 'Sec-Fetch-Site': 'same-site',
# # 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
#   'X-User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 FKUA/website/42/website/Desktop',
#   'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Brave";v="126"',
# # 'sec-ch-ua-mobile': '?0',
# # 'sec-ch-ua-platform': '"Linux"'
# }

# response = requests.request("POST", url, headers=headers, data=payload)

# print(response)
# print(response.text)


# response = requests.request("POST", url, headers=headers, data=payload)

# print(response.text)
import httpx
import json
def scrap(FSN):
    
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
        response_code=response.status_code
        response_header=response.headers
        df=response.text
    except httpx.HTTPStatusError as http_err:
        pass
        # print(f"HTTP error occurred: {http_err}")  # Python 3.6
        # print(f"Response status code: {response.status_code}")
        # print(f"Response body: {response.text}")
    except Exception as err:
        # print(f"Other error occurred: {err}")
        pass  # Python 3.6
    return json.loads(response.text)

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
def title_scrap(FSN):
    data=scrap(FSN)
    title_text=title(data)
    return title_text