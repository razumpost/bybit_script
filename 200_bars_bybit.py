import requests
import pandas as pd
import warnings
import datetime as dt
import time
import numpy as np


def kline4(symb, tf, N):

    url = "https://api.bybit.com"
    path = "/v5/market/kline"
    URL = url + path
    end_ds = int(time.time() * 1000)



    batch_size = 200
    dtf = int(tf) * 60 * 1000

    start_ds = end_ds - N*dtf
    batch_count = N // batch_size + int(N % batch_size != 0)

    dfs = pd.DataFrame()
    for i in range(batch_count):
        batch_start = start_ds + i * batch_size*dtf
        batch_end = min(end_ds, batch_start + batch_size*dtf)

        params = {'category': 'linear', "symbol": symb, "interval": tf, 'start': batch_start, 'end': batch_end}
        r = requests.get(URL, params=params)
        df = pd.DataFrame(r.json()['result']['list'])
        # pd.set_option('max_columns', None)
        m = pd.DataFrame()
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            m['Date'] = df.iloc[:, 0].astype(np.int64)
            m['Date'] = pd.to_datetime(m['Date'], unit="ms")
            m['Open'] = df.iloc[:, 1].astype(float)
            m['High'] = df.iloc[:, 2].astype(float)
            m['Low'] = df.iloc[:, 3].astype(float)
            m['Close_BTC'] = df.iloc[:, 4].astype(float)
            m['Volume'] = df.iloc[:, 5].astype(float)
            m = m.sort_values(by='Date')

        dfs = pd.concat([dfs, m], ignore_index=True)
    return dfs


result = kline4('BTCUSDT', 5, 1500)
close_column = result['Close_BTC']

print(close_column)
