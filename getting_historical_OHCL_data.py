#simple function to query and retrieve OHLC from AV's API
#note that the API key needs to be entered instead of KEY
#bulk requests as this may result in termination
#sleep of 20 secs seems appropriate

def get_data_from_Alpha_Vantage():

#note that some source of ticker names may be passed here
#instead of single ticker names
#list of different exchanges and marketplaces will follow
    
   for ticker in tickers:
        link = ('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s&outputsize=full&apikey=KEY' % ticker)
        re = requests.get(link)
        df = pd.DataFrame.from_dict(re.json())
        df.drop('Meta Data', axis = 1, inplace = True)
        df.reset_index(inplace=True)
        df = df.dropna()
        ohlc = pd.DataFrame(df["Time Series (Daily)"].dropna().tolist() )
        df = df.join(ohlc)
        df.drop(['Time Series (Daily)'], axis = 1, inplace = True)
        df.to_csv('stock_dfs/%s.csv' % ticker)
        time.sleep(20)