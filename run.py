
from typing import OrderedDict
import pandas as pd
import pprint
import os.path 
import logging 
import time


import os
from datetime import date, timedelta
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.keys import Keys
# For waits
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# chrome options to keep browser open 
from selenium.webdriver.chrome.options import Options
chrome_options = Options() 



# Forex Lib
import yfinance as yf
from datetime import datetime

CHROMEDRIVER_PATH  = "../tools/chromedriver"

torexe = os.popen(r'/usr/sbin')
PROXY = "socks5://localhost:9050" # IP:PORT or HOST:PORT
chrome_options.add_argument(f'--proxy-server={PROXY}')

# Setup logger
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)


# Log to console
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

#Silence all logger
loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
for logger in loggers:
    logger.setLevel(logging.WARNING)

logging.info("Checking path to chromedriver")
res = os.path.exists(CHROMEDRIVER_PATH)
if res is not True:
    error_string = f"The path for the Chrome driver is not valid. {CHROMEDRIVER_PATH}"
    logging.critical(error_string)
    raise ValueError(error_string)

logging.info("Chromedriver pass found")

class ScraperBaseClass():

    def __init__(self):
        logging.info("Starting CME Scraper")
        try:
            self.driver = webdriver.Chrome(ChromeDriverManager().install())

            # self.driver = webdriver.Chrome(CHROMEDRIVER_PATH,options=chrome_options)
        except Exception as e:
            error_string = f"Failed to create webdriver object \n {e}"
            logging.error(error_string)
            raise Exception(error_string)


class CMEScraper(ScraperBaseClass):
    # cmeDelayedQuotes2_CLN1_open
    # cmeDelayedQuotes2_CLQ1_open
    
    CME_CRUDE_OIL_PATH = 'https://www.cmegroup.com/market-data/delayed-quotes/energy.html'
    CME_AGRI_PATH = 'https://www.cmegroup.com/market-data/delayed-quotes/agricultural.html'
    def __init__(self):
        super().__init__()
        

    def get_crude_oil_futures(self):
        self.driver.get(CMEScraper.CME_CRUDE_OIL_PATH)
        logging.info("Getting crude oil futures")
        data = OrderedDict()
        open_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_CLQ1_open"))
            ).text
        high_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_CLQ1_high"))
            ).text
        low_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_CLQ1_low"))
            ).text
      
        logging.info(f"OIL FUTURES PRICES: O:{open_price} H:{high_price} L:{low_price}")

        data['crude_oil'] =  {'open': open_price, 'high': high_price,'low' : low_price}
        return data

    def get_corn_futures(self):
        logging.info("Getting corn futures")
        data = OrderedDict()
        self.driver.get(CMEScraper.CME_AGRI_PATH)
        open_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZCZ1_open"))
            ).text
        high_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZCZ1_high"))
            ).text
        low_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZCZ1_low"))
            ).text
      
        logging.info(f"CORN FUTURES PRICES: O:{open_price} H:{high_price} L:{low_price}")

        data['corn_futures'] =  {'open': open_price.replace('\'','.'), 'high': high_price.replace('\'','.'),'low' : low_price.replace('\'','.')}
        return data

    def get_soybean_futures(self):
        logging.info("Getting soybeans futures")
        data = OrderedDict()
        self.driver.get(CMEScraper.CME_AGRI_PATH)
        open_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZSX1_open"))
            ).text
        high_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZSX1_high"))
            ).text
        low_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZSX1_low"))
            ).text
      
        logging.info(f"SOYBEAN FUTURES PRICES: O:{open_price} H:{high_price} L:{low_price}")

        data['soybean_futures'] =  {'open': open_price.replace('\'','.'), 'high': high_price.replace('\'','.'),'low' : low_price.replace('\'','.')}
        return data

    def get_soybean_meal_futures(self):
        logging.info("Getting soybeans meal futures")
        data = OrderedDict()
        self.driver.get(CMEScraper.CME_AGRI_PATH)
        open_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZMZ1_open"))
            ).text
        high_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZMZ1_high"))
            ).text
        low_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZMZ1_low"))
            ).text
      
        logging.info(f"SOYBEAN MEAL FUTURES PRICES: O:{open_price} H:{high_price} L:{low_price}")

        data['soybean_meal_futures'] =  {'open': open_price.replace('\'','.'), 'high': high_price.replace('\'','.'),'low' : low_price.replace('\'','.')}
        return data

    def get_soybean_oil_futures(self):
        logging.info("Getting soybeans oil futures")
        data = OrderedDict()
        self.driver.get(CMEScraper.CME_AGRI_PATH)
        open_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZLZ1_open"))
            ).text
        high_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZLZ1_high"))
            ).text
        low_price = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "cmeDelayedQuotes2_ZLZ1_low"))
            ).text
      
        logging.info(f"SOYBEAN OIL FUTURES PRICES: O:{open_price} H:{high_price} L:{low_price}")

        data['soybean_oil_futures'] =  {'open': open_price.replace('\'','.'), 'high': high_price.replace('\'','.'),'low' : low_price.replace('\'','.')}
        return data

class BarchartScraper(ScraperBaseClass):

    PALM_OIL_PATH = 'https://www.barchart.com/futures/prices-by-exchange/mdex?future=KO'
    SOYBEAN_PATH  = 'https://www.barchart.com/futures/quotes/ZS*0/futures-prices'
    CANOLA_PATH   = 'https://www.barchart.com/futures/quotes/RS*0/futures-prices'
    GRAINS_PATH   = 'https://www.barchart.com/futures/european/grains'

    def __init__(self):
        super().__init__()
        # Get MYRUSD=x
        self.conversion = myr2usd()
    

    def get_palm_oil_derivative(self):
        self.driver.get(BarchartScraper.PALM_OIL_PATH)
        table = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/main/div/div[2]/div[2]/div/div[2]/div/div/div/div[3]/div/div[2]/div/div/ng-transclude/table'))
            )
        palmOilData = OrderedDict()
        for i,row in enumerate(table.find_elements_by_tag_name('tr')):

            # logger.info("fetching next row")
            # Thiis will get and convert each price from MYR to USD
            parsed_row = [value.text for value in row.find_elements_by_tag_name('td')]
          
            if len(parsed_row)>0:
                # x = float(parsed_row[4].replace(',',''))
                # print(x)
                # temp = float(parsed_row[4].replace(',',''))/self.conversion
                try:
                    data = {'month' : parsed_row[1],
                            'open': float(parsed_row[4].replace(',',''))*self.conversion,
                            'high': float(parsed_row[5].replace(',',''))*self.conversion,
                            'low' : float(parsed_row[6].replace(',',''))*self.conversion
                            }
                    palmOilData[data['month']] = data
                except Exception as e:
                    pass

        return palmOilData

    
    def get_canola_futures(self):
        self.driver.get(BarchartScraper.CANOLA_PATH)
        canolaFuturesData = OrderedDict()
        table = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/main/div/div[2]/div[2]/div/div[2]/div/div/div/div[6]/div/div[2]/div/div/ng-transclude/table'))
            )

        for i,row in enumerate(table.find_elements_by_tag_name('tr')):
            # logging.info("fetching next row")
            parsed_row = [value.text for value in row.find_elements_by_tag_name('td')]
            if len(parsed_row)>0:
                try:
                    data = {'month' : parsed_row[0],
                            'open': parsed_row[3],
                            'high': parsed_row[4],
                            'low' : parsed_row[5]}
                    canolaFuturesData[data['month']] = data
                except Exception as e:
                    pass

        return canolaFuturesData

    def get_rapeseed_futures(self):
        logging.info("Fetching rapeseed futures")
        rapeseedFuturesData = OrderedDict()
        self.driver.get(BarchartScraper.GRAINS_PATH)
        table = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/main/div/div[2]/div[2]/div/div[2]/div/div/div/div[5]/div[2]/div[2]/div/div/ng-transclude/table'))
            )

        for i,row in enumerate(table.find_elements_by_tag_name('tr')):
            # logging.info("fetching next row")
            parsed_row = [value.text for value in row.find_elements_by_tag_name('td')]
            if len(parsed_row)>0:
                try:
                    if parsed_row[1].startswith("Rapeseed"):
                        data = {'month' : parsed_row[1],
                                'open': parsed_row[4],
                                'high': parsed_row[5],
                                'low' : parsed_row[6]}
                    rapeseedFuturesData[data['month']] = data
                except Exception as e:
                    pass

        return rapeseedFuturesData


    def get_milling_wheat_futures(self):
        logging.info("Fetching milling wheat futures")
        millingWheatFuturesData = OrderedDict()
        self.driver.get(BarchartScraper.GRAINS_PATH)
        table = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/main/div/div[2]/div[2]/div/div[2]/div/div/div/div[5]/div[2]/div[2]/div/div/ng-transclude/table'))
            )

        for i,row in enumerate(table.find_elements_by_tag_name('tr')):
            # logging.info("fetching next row")
            parsed_row = [value.text for value in row.find_elements_by_tag_name('td')]
            if len(parsed_row)>0:
                try:
                    if parsed_row[1].startswith("Milling Wheat"):
                        data = {'month' : parsed_row[1],
                                'open': parsed_row[4],
                                'high': parsed_row[5],
                                'low' : parsed_row[6]}

                    millingWheatFuturesData[data['month']] = data

                except Exception as e:
                    pass
        return millingWheatFuturesData


    def get_corn_futures(self):
        logging.info("Fetching corn futures")
        cornFuturesData = OrderedDict()
        self.driver.get(BarchartScraper.GRAINS_PATH)
        table = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/main/div/div[2]/div[2]/div/div[2]/div/div/div/div[5]/div[2]/div[2]/div/div/ng-transclude/table'))
            )

        for i,row in enumerate(table.find_elements_by_tag_name('tr')):
            # logging.info("fetching next row")
            parsed_row = [value.text for value in row.find_elements_by_tag_name('td')]
            if len(parsed_row)>0:
                try:
                    if parsed_row[1].startswith("Corn"):
                        data = {'month' : parsed_row[1],
                                'open': parsed_row[4],
                                'high': parsed_row[5],
                                'low' : parsed_row[6]}

                    cornFuturesData[data['month']] = data

                except Exception as e:
                    pass
        return cornFuturesData


class resultsHandler():

    def __init__(self):
        pass

    def save_palm_oil(self,palm_oil_data):
        pass
    
    def select_month(self,data,title):
        print("************************************************")
        logging.info(f"The following data was found for <{title}>:")
        input("Press any key to display")
        for i,k in enumerate(list(data.keys())):
            print(f"{i}  :: {k}")
        
        print("************************************************")
        index = input(f"Select <{title}> date (type number): \n")
        index = int(index)
        print("************************************************")
        logging.info(f"You have selected: {index}")
        print("************************************************")
        # get the index, convert to key display vals
        key = list(data.keys())[index]
        return data[key]

    def save(self,data):
        # Open a file and save the data]
        print(data)
        title = list(data.keys())[0]
        o     = str(data[title]['open'])
        h     = str(data[title]['high'])
        l     = str(data[title]['low'])
        with open(FILEPATH, 'a') as fh:
            fh.write(f"{title} open: {o}  high: {h} low: {l} \n")

    def save_barchart(self,data):
        title = data['month']
        o     = str("{:.2f}".format(float(data['open']))) 
        h     = str("{:.2f}".format(float(data['high']))) 
        l     = str("{:.2f}".format(float(data['low']))) 
        with open(FILEPATH, 'a') as fh:
            try:
                fh.write(f"{title} open: {o}  high: {h} low: {l} \n")
            except Exception as e:
                print(e)





def myr2usd():
    date = datetime.today().strftime('%Y-%m-%d')
    myrusd_raw = yf.download(tickers = 'MYRUSD=X',start=date)

    myrusd = (myrusd_raw.loc[:,"Open"][0])
    return myrusd


def yforex():
    # yf.pdr_override() 

    date = datetime.today().strftime('%Y-%m-%d')
    usdeur_raw = yf.download(tickers = 'USDEUR=X',start=date)
    myrusd_raw = yf.download(tickers = 'MYRUSD=X',start=date)

    usdeuro = (usdeur_raw.loc[:,"Open"][0])
    myrusd = (myrusd_raw.loc[:,"Open"][0])
    print(usdeuro)
    print(myrusd)


            
        

if __name__ == "__main__":
    date = datetime.today().strftime('%Y_%m_%d-%H_%M_%S')
    FILEPATH = os.getcwd() + '_' + date + '.txt'

    cme_scraper = CMEScraper()
    co = cme_scraper.get_crude_oil_futures()
    cf = cme_scraper.get_corn_futures()
    sb = cme_scraper.get_soybean_futures()
    sm = cme_scraper.get_soybean_meal_futures()
    so = cme_scraper.get_soybean_oil_futures()
    cme_scraper.driver.close()

    barchart_scraper = BarchartScraper()
    po = barchart_scraper.get_palm_oil_derivative()
    ca = barchart_scraper.get_canola_futures()
    ra = barchart_scraper.get_rapeseed_futures()
    wh = barchart_scraper.get_milling_wheat_futures()
    corn = barchart_scraper.get_corn_futures()
    barchart_scraper.driver.close()


    # Class to handle results 
    rh = resultsHandler()

    #  sav CME results 
    rh.save(co)
    rh.save(cf)
    rh.save(sb)
    rh.save(sm)
    rh.save(so)

    po  = rh.select_month(po,'Palm Oil')
    can = rh.select_month(ca,'Canola')
    ra  = rh.select_month(ra, 'Rapeseed')
    mw  = rh.select_month(wh, 'Milling Wheat')
    crn = rh.select_month(corn, 'Corn')
    
    rh.save_barchart(po)
    rh.save_barchart(can)
    rh.save_barchart(ra)
    rh.save_barchart(mw)
    rh.save_barchart(crn)
