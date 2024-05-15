from entsoe import EntsoePandasClient
import pandas as pd
import polars as pl
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from data_analyzer import DataAnalyzer

def create_prices_csv():
    load_dotenv()
    api_key = os.getenv("ENTSOE_API_KEY")
    client = EntsoePandasClient(api_key=api_key)

    start = pd.Timestamp('2023-11-01', tz='Europe/Helsinki')
    end = pd.Timestamp('2024-05-14', tz='Europe/Helsinki')
    country_code = 'FI'

    # methods that return Pandas Series
    ts = client.query_day_ahead_prices(country_code, start=start, end=end)
    ts.to_csv('prices.csv')


def create_dataframe():
    # Scan CSV file for price dataframe
    df_prices = pl.scan_csv(
        'prices.csv',
        new_columns=["ts", "price"],
        dtypes=[pl.Datetime, pl.Float32],
    ).collect()
    df_prices = df_prices.with_columns(pl.col('ts').dt.hour().alias('hour'))
    df_prices = df_prices.with_columns(pl.col('ts').dt.date().alias('date'))

    # Scan parquet file for data dataframe
    df_data = pl.scan_parquet('all_data_compressed_zstd_1698789601000-1715547590000.parquet').collect()
    df_data = df_data.with_columns(pl.col('ts').dt.hour().alias('hour'))
    df_data = df_data.with_columns(pl.col('ts').dt.date().alias('date'))

    # Merge df_prices['price'] into df_data based on 'hour' and 'date' columns
    df_all_data = df_data.join(df_prices.select(['hour', 'date', 'price']), on=['hour', 'date'], how='left')
    df_all_data = df_all_data.rename({'ts': 'ts_micro'})
    df_all_data = df_all_data.with_columns(ts=pl.col('ts_micro').cast(pl.Datetime('ms')))
    df_all_data = df_all_data.drop('ts_micro')
    df_all_data = df_all_data.drop('hour')
    df_all_data = df_all_data.drop('date')

    selected_columns = ['ts', 'meter_id'] + [col for col in df_all_data.columns if col != 'ts' and col != 'meter_id']
    df_all_data = df_all_data.select(selected_columns)
    return df_all_data
    # Save all data to parquet file
    # df_all_data.write_parquet('all_data_with_price.parquet', compression='gzip')


def show_options():
    choice = input('\n1. list columns\n2. sample\n3. describe\n4. SQL query\nq. exit\naction: ')
    return choice


def main():
    df = create_dataframe()
    print(df.head())
    print(f'Rows: {df.height}')

    analyzer = DataAnalyzer(df)

    while True:
        action = show_options()

        if action == '1':
            print('\nColumns in the DataFrame:')
            print(analyzer.list_columns())
        if action == '2':
            print('\nSample of the DataFrame:')
            print(analyzer.show_sample())
        if action == '3':
            print("\nSummary statistics of the DataFrame:")
            print(analyzer.describe_dataframe())
        if action == '4':
            analyzer.query_with_sql()
        if action == 'q':
            # Plotting time vs. price
            plt.figure(figsize=(10, 6))
            plt.plot(df['ts'].to_numpy(), df['price'].to_numpy(), linestyle='-')
            plt.title('Price Over Time')
            plt.xlabel('Date')
            plt.ylabel('Price')
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
            break


if __name__ == '__main__':
    main()
