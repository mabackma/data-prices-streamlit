from entsoe import EntsoePandasClient
import pandas as pd
import polars as pl
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from data_analyzer import DataAnalyzer
import streamlit as st


# Function to read large parquet file
def read_large_parquet(file):
    df = pd.read_parquet(file)
    return df


st.set_page_config(layout="wide")
st.write('<h3>Place your parquet file here</h3>', unsafe_allow_html=True)

# File uploader for parquet file
uploaded_file = st.file_uploader("Choose a file", type=["parquet"])

if uploaded_file is not None:
    # Display file details
    st.write("Filename:", uploaded_file.name)
    st.write("File size:", uploaded_file.size, "bytes")

    # Read and process the file
    with st.spinner('Reading the file...'):
        df = read_large_parquet(uploaded_file)



    st.success('File successfully read!')

    # Create df_L1_L2_L3 and df_total dataframes
    total_columns = ['Total current', 'Total active power', 'Total apparent power',
                           'Total active energy', 'Total active returned energy']
    df_L1_L2_L3 = df.drop(total_columns, axis=1)
    data_columns = ['L1 current', 'L1 voltage', 'L1 active power', 'L1 apparent power', 'L1 Power factor',
                        'L1 frequency', 'L1 total active energy', 'L1 total active returned energy',
                        'L2 current', 'L2 voltage', 'L2 active power', 'L2 apparent power', 'L2 Power factor',
                        'L2 frequency', 'L2 total active energy',
                        'L3 current', 'L3 voltage', 'L3 active power', 'L3 apparent power', 'L3 Power factor',
                        'L3 frequency', 'L3 total active energy', 'L3 total active returned energy_right']
    df_total = df.drop(data_columns, axis=1)

    st.write('<h3>L1, L2, L3 values:</h3>', unsafe_allow_html=True)
    st.write(df_L1_L2_L3.head())
    st.write('<h3>Total values:</h3>', unsafe_allow_html=True)
    st.write(df_total.head())
'''''
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
'''''
