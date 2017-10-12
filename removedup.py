import pandas as pd
from sqlalchemy import create_engine
from pandas.util.testing import assert_frame_equal

import time


if __name__ == "__main__":
    start_time = time.process_time()

    path = 'C:/Users/Potterhead/Downloads/Zeta_Wallet_Loads_Zeta_Loads.csv'
    #df = pd.read_csv(path, nrows=5)
    df = pd.read_csv(path)
    df.columns = map(str.lower, df.columns)
    df = df.fillna(1)
    df = df.astype(dtype={'balance': 'float', 'load #': 'float', 'dmi user bal': 'float', 'rc user balance': 'float',
                          'dmi user bal post may': 'float', 'unnamed: 16': 'float', 'actual balnce': 'float'},
                   errors='ignore')
    #query = 'Select * from mytable limit 5'
    query = 'Select * from mytable'
    for chunks in pd.read_sql(query, create_engine('postgresql://postgres:saksham007@localhost:5432/postgres'),
                              chunksize=10000):
        df2 = chunks
    df2 = df2.astype(dtype={'balance': 'float', 'load #': 'float', 'dmi user bal': 'float', 'rc user balance': 'float',
                            'dmi user bal post may': 'float', 'unnamed: 16': 'float', 'actual balnce': 'float'},
                     errors='ignore')
    #print(df2['bank ref txn id'] == df['bank ref txn id'])
    #df3 = pd.merge(df, df2, on=['date', 'from / to party', 'credit', 'debit', 'balance', 'bank ref txn id', 'load #', 'key', 'lender', 'zeta order id', 'dmi user bal', 'rc user balance', 'dmi user bal post may', 'chandan comment', 'current date', 'status', 'unnamed: 16', 'actual balnce'], how='inner')
    #print(df3)
    df['chandan comment'] = df['chandan comment'].apply(str)
    df['bank ref txn id'] = df['bank ref txn id'].apply(str)
    df['zeta order id'] = df['zeta order id'].apply(str)
    df['from / to party'] = df['from / to party'].apply(str)
    df2 = df2.fillna(1)
    print(df.get_value(122,'lender') == df2.get_value(122,'lender'))
    print(type(df.get_value(122,'lender')))
    print(type(df2.get_value(122, 'lender')))
    l = []
    l2 = []
    for i in range(0, 6399):
        if df.get_value(i, 'lender') == df2.get_value(i, 'lender'):
            l2.append(i)
        else:
            l.append(i)
    print("different values row number")
    print(l)
    print("duplicated values row number")
    print(l2)
    print("--- %s seconds ---" % (time.process_time() - start_time))
    #print(df['chandan comment'])
    #print(df2['chandan comment'])
    #print(df)

    #df3 = df.head(10)
    #df4 = df2.head(10)
    #assert_frame_equal(df3, df4)
    #print(df3.iloc[:10] == df4.iloc[:10])
    #data = pd.merge(df3, df4, how='inner', on='credit')
    #print(data)
