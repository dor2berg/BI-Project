import pandas as pd
import sqlite3

class NetflixETL:
    def __init__(self):
        self.price_data = None
        self.subscription_data = None

    def extract_price_data(self):
        self.price_data = pd.read_csv('netflix price in different countries.csv', encoding='latin-1')
        self.price_data.dropna(inplace=True)
        print("Price data column names:")
        print(self.price_data.columns)

    def extract_subscription_data(self):
        self.subscription_data = pd.read_csv('Netflix subscription fee Dec-2021.csv', encoding='latin-1')
        self.subscription_data.dropna(inplace=True)
        print("Subscription data column names:")
        print(self.subscription_data.columns)

    def transform_data(self):
        # Modify this part based on the actual column names in your dataset
        merged_data = pd.merge(self.price_data, self.subscription_data, on='Country')
        merged_data['Total Cost per Month'] = merged_data['Cost Per Month - Basic ($)'] + merged_data['Cost Per Month - Standard ($)'] + merged_data['Cost Per Month - Premium ($)']
        profitable_program = merged_data.groupby('Country')['Total Cost per Month'].idxmax()
        transformed_data = merged_data.loc[profitable_program, ['Country', 'Total Cost per Month']]
        return transformed_data

    def load_data(self):
        conn = sqlite3.connect('netflix_profitability.db')
        transformed_data = self.transform_data()
        transformed_data.to_sql('profitability_data', conn, if_exists='replace', index=False)
        conn.close()
        print("Profitability data successfully loaded to the database.")
        print(transformed_data)

    def run_etl(self):
        self.extract_price_data()
        self.extract_subscription_data()

        self.load_data()

# Instantiate the NetflixETL class
etl = NetflixETL()

# Run the ETL process
etl.run_etl()
