#imports
import os
import sys
import requests
import pandas as pd
import time

gemtable = pd.read_excel('stock_dfs/GEM/gem comps.xlsx')
tickers = gemtable["CODE"].tolist()

#the actual function for getting separate files for each listed company
def get_OHLC_data(companies):

    #have it saved in a specific folder
    if not os.path.exists('stock_dfs/GEM'):
        os.makedirs('stock_dfs/GEM')

    tickers = tickers

    #kindly use your own API key
    for ticker in tickers:
        try:
            link = ('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s.HK&outputsize=full&apikey=YOURKEYHERE' % ticker)
            re = requests.get(link)
            #the resulting DF may be handled for simple, non-nested indices
            df = pd.DataFrame.from_dict(re.json())
            df.drop('Meta Data', axis = 1, inplace = True)
            df.reset_index(inplace=True)
            df = df.dropna()
            ohlc = pd.DataFrame(df["Time Series (Daily)"].dropna().tolist() )
            df = df.join(ohlc)
            df.drop(['Time Series (Daily)'], axis = 1, inplace = True)
            df.to_csv('stock_dfs/GEM/%s.csv' % ticker)
            #be patient with the API. It is free, remember?
            time.sleep(20)
        except:
            #we need this exception since there are some errors raised in case the API won't recognize the code in ther DB
            continue
        
get_OHLC_data(companies)
