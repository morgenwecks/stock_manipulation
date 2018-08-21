#for gem companies in HK, both initial frames and ongoing update, for which the call size is shorter

def fetch_GEM_data():
    
    import requests
    import pandas as pd
    import time
    import os
    
    gemtable = pd.read_excel('stock_dfs/gem_comps.xlsx')
    tickers = gemtable["CODE"].tolist()
    
    dumptickers = []
    
    if not os.path.exists('stock_dfs/GEM'):
        os.makedirs('stock_dfs/GEM')
          
    for ticker in tickers:
        print("Now at: %s" % ticker)
        try:
            df_old = pd.read_csv('stock_dfs/GEM/%s.csv' % ticker)
            try:
                link = ('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s.HK&outputsize=compact&apikey=XHUW4LB158S6333F' % ticker)
                re = requests.get(link)
            
                df_recent = pd.DataFrame.from_dict(re.json())
                df_recent.drop(['Meta Data'], axis =1, inplace = True)
                df_recent.drop(df.index[[0,1,-1,-2,-3]], inplace = True)
                df_recent.reset_index(inplace = True)
                            
                ohlc = pd.DataFrame(df_recent['Time Series (Daily)'].dropna().tolist() )
                df_recent = pd.merge(df_recent, ohlc, left_index=True, right_index=True)
                df_recent.drop(['Time Series (Daily)'], axis = 1, inplace = True)
            
                df_old = pd.read_csv('stock_dfs/GEM/%s.csv' % ticker)
                df_fresh = df_old.append(df_recent, sort = True)
                df_fresh.drop_duplicates(inplace = True)
                df_fresh = df_fresh[['index','1. open', '2. high', '3. low', '4. close', '5. volume']]
                cols = ['date','open','high','low','close','volume']
                df_fresh.columns = cols
                df_fresh.to_csv('stock_dfs/GEM/%s.csv' % ticker)
                time.sleep(12)
            except Exception as e:
                print(e)
                print(re.text)
                dumptickers.append(ticker)
                time.sleep(12)
                continue
        except Exception as e:
            print(e)
            print('attempt to create new file for %s and write ticker name to dumptickers list' % ticker)
            try:
                link = ('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s.HK&outputsize=full&apikey=XHUW4LB158S6333F' % ticker)
                re = requests.get(link)
                
                df_init = pd.DataFrame.from_dict(re.json())
                df_init.drop(['Meta Data'], axis =1, inplace = True)
                df_init.drop(df.index[[0,1,-1,-2,-3]], inplace = True)
                df_init.reset_index(inplace = True)
                            
                ohlc = pd.DataFrame(df_init['Time Series (Daily)'].dropna().tolist() )
                df_init = df_init.join(ohlc)
                df_init.drop(['Time Series (Daily)'], axis = 1, inplace = True)
                df_init.to_csv('stock_dfs/GEM/%s.csv' % ticker)
                time.sleep(12)
            except Exception as e:
                print(e)
                print(re.text)
                dumptickers.append(ticker)
                time.sleep(12)
                continue
            continue
      
    return(dumptickers)

