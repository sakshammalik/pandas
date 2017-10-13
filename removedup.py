import pandas as pd
from sqlalchemy import create_engine

if __name__ == "__main__":
    file = 'C:/Users/Potterhead/Downloads/Zeta_Wallet_Loads_Zeta_Loads.csv'
    engine = create_engine('postgresql://postgres:saksham007@localhost:5432/postgres')
    df = pd.read_csv(file)

    #df.index += 1
    df.columns = map(str.lower, df.columns)
    df[['chandan comment', 'zeta order id']] = df[['chandan comment', 'zeta order id']].fillna('0')
    df[['debit']] = df[['debit']].fillna(0.0)
    df.to_sql('mytable', engine, if_exists='replace', index=False)
    query = 'SELECT * from mytable'
    df2 = pd.read_sql_table('mytable', con=engine)
    # df = df.head(100)
    # df2 = df2.head(100)
    # df2.iloc[0].equals(df.iloc[0]))
    i = 0
    j = 0
    c = 0
    for i in range(0, 1000):
        for j in range(0, 1000):
            if df2.iloc[i].equals(df.iloc[j]):
                c += 1
                break
            else:
                pass
    print(c)

        # print(l)
    # print(df2.get_value(1075, 'from / to party'))
    # l = []
    # l2 = []
    # for i in range(1, 6400):
    #     if df.get_value(i, 'from / to party') == df2.get_value(i, 'from / to party'):
    #         l2.append(i)
    #     else:
    #         l.append(i)
    # for j in l2:
    #     print(df.get_value(j, 'from / to party'))
