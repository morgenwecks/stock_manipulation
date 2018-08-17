#imports
import os
import sys
import requests
import bs4 as bs
import pandas as pd
import time

#scraping HKEx for recent list of GEM companies
resp = requests.get('http://www.hkexnews.hk/hyperlink/hyperlist_gem.HTM')
soup = bs.BeautifulSoup(resp.text, 'lxml')
table = soup.find('table', {'class': 'table_grey_border'})

#as that will be a useful code: [name,link] structure, we want a dict
companies = {}
entries = table.find_all('tr', {'class':'tr_normal'})


#extracting the code,name,link from each row and pass it to our dict
#we might want to pickle this so it doesnt have to scrape every time, as hk websites are naturally fluid and not loaded with logic
for entry in entries:
    scode = list(entry)[1].text
    #there are more or less arbitrary new lines in the codes
    if len(scode) > 4:
        scode = scode[1:]
    sname = list(single)[3].text
    slink = list(single)[5].text
    companies[scode] = [sname,slink]

#the actual function for getting separate files for each listed company
def get_OHLC_data(companies):

    #have it saved in a specific folder
    if not os.path.exists('stock_dfs/GEM'):
        os.makedirs('stock_dfs/GEM')

    #turn the key object into an interable list    
    tickers = list(companies.keys())

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
