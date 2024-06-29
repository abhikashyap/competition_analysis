from selenium.webdriver.common.by import By
import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


from piTask import general
from selenium.webdriver.support.wait import WebDriverWait

extension_file = "Helium 10 8.3.0.0.crx"
extension_path = os.path.join(os.getcwd(), "extensions", extension_file)


class WebDriverManager:
    def __init__(self, address=general.address):
        self.address = address

    def configure_driver(self):
        options = Options()
        options.add_argument("driver-infobars")
        options.add_argument("start-maximized")
        options.add_argument("disable-dev-shm-usage")
        options.add_argument("no-sandbox")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("disable-blink-features=AutomationControlled")
        options.add_argument(f"--force-device-scale-factor=0.70")
        options.add_extension(extension_path)
        # options.add_argument('--headless')
        options.add_experimental_option(
            "prefs",
            {
                "profile.password_manager_enabled": False,
                "credentials_enable_service": False,
                "download.default_directory": self.address,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )

        service = Service()
        driver = webdriver.Chrome(options=options, service=service)
        driver.implicitly_wait(2)
        return driver


def helium10_extension(asins):
    driver = WebDriverManager().configure_driver()
    driver.close()
    driver.switch_to.window(driver.window_handles[0])
    driver.get("https://members.helium10.com/user/signin")
    time.sleep(2)
    helium10_credentials = {
        'Email': 'Ajay.m@blooprint.in',
        'Password': 'bpecomhelium10@123'
    }

    email = driver.find_element(By.CSS_SELECTOR, '[id="loginform-email"]')
    email.send_keys(helium10_credentials['Email'])
    time.sleep(1)

    password = driver.find_element(By.CSS_SELECTOR, '[id="loginform-password"]')
    password.send_keys(helium10_credentials['Password'])
    time.sleep(1)

    login = driver.find_element(By.CSS_SELECTOR, '[type="submit"]')
    login.click()
    time.sleep(10)

    list = []
    for asin in asins:
        print(asin)
        driver.get(f"https://www.amazon.in/Rabbit-Convertible-Feeding-Shampoo-Bathing/dp/{asin}/ref=sr_1_1_sspa?dib=eyJ2IjoiMSJ9.vtY_exlPEIgwnPsfst1TbiQmjK1w0C67JutuK1ROGyesaWKkOB3_XoNWCcrVWUjLSyvnXBgrW-HQR5_EWRiFUpnsEWl4u5hBnaHi6-DTo3-H2TUetzAyI646BRJget6NIQR8e0yPj0_JgwfNlxUgUyDTCgquRgyzdZhDpG72nt9pxdGAq12PzjWst_ZQASfGUhp7Aa-LbLBPR7ibXVKtlL5qnY3J8IXEBEYp3lCt2gICj7KZgWZf9UI-oxHluDUrusO6vxCnuK4dZnRE_SkqhpoAxjjbsZWSAKbyQg0J8Sk.5Z304YfsAnkEV1HD2FLw3hU9vti-WzZeCW-cVqQOD8Q&dib_tag=se&keywords=Kids%2Bfurnishings&pf_rd_i=1380442031&pf_rd_m=A1VBAL9TL5WCBF&pf_rd_p=96723119-b73f-4f8a-86db-3768c23bb81c&pf_rd_r=AY6CSTJ6QGR0SRXVENDE&pf_rd_s=merchandised-search-6&qid=1719490605&sr=8-1-spons&sp_csd=d2lkZ2V0TmFtZT1zcF9hdGY&th=1")
        driver.implicitly_wait(10)
        sales = driver.find_element(By.CSS_SELECTOR, '[class*="sc-ePCPUn IHfQR"]')
        wait = WebDriverWait(driver, timeout=30)
        wait.until(lambda d: sales.is_displayed())
        sales = sales.text.replace("₹", "").replace(",", "")
        if sales != 'N/A':
            sales = int(float(sales))
        units = driver.find_element(By.CSS_SELECTOR, '[class="sc-zbfRe eoTvuN"]').text.replace(",", "")
        dict = {
            'ASIN': asin,
            'Sales': sales,
            'Units': units
        }
        list.append(dict)

    df = pd.DataFrame(list)
    driver.quit()
    return df
