import unittest
import pandas as pd
from sqlalchemy import create_engine
from pandas.util.testing import assert_frame_equal


class MyTestCase(unittest.TestCase):

    def setUp(self):
        file = 'C:/Users/Potterhead/Downloads/Zeta_Wallet_Loads_Zeta_Loads.csv'
        engine = create_engine('postgresql://postgres:saksham007@localhost:5432/postgres')
        df = pd.read_csv(file)
        df.columns = map(str.lower, df.columns)
        df[['credit', 'debit']] = df[['credit', 'debit']].fillna(0)
        df.to_sql('demooo', engine, if_exists='replace')

    def test_sum(self):
        query = 'SELECT sum(credit) as credit, sum(debit) as debit from demooo'
        for chunks in pd.read_sql(query, create_engine('postgresql://postgres:saksham007@localhost:5432/postgres'),
                                  chunksize=100000):  # reads data in chunks of 1000 rows
            df2 = chunks
        df1 = pd.DataFrame({"credit": [27453215.89], "debit":[27364275.7]})
        assert_frame_equal(df2, df1)


if __name__ == '__main__':
    unittest.main()
