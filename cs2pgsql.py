import pandas as pd
from sqlalchemy import create_engine

if __name__ == "__main__":
    file = 'C:/Users/Potterhead/Downloads/Zeta_Wallet_Loads_Zeta_Loads.csv'
    engine = create_engine('postgresql://postgres:saksham007@localhost:5432/postgres')
    df = pd.read_csv(file)
    df.columns = map(str.lower, df.columns)
    df[['credit', 'debit']] = df[['credit', 'debit']].fillna(0)
    df.to_sql('demooo', engine, if_exists='replace')

    if df.size:
        print("Success!")
    query = 'SELECT sum(credit), sum(debit) from demooo'  # calculates the sum
    for chunks in pd.read_sql(query, engine, chunksize=1000):  # reads data in chunks of 1000 rows
        print(chunks)  # prints the sum

