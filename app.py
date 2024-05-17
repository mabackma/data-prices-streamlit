from entsoe import EntsoePandasClient
import pandas as pd
import polars as pl
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from data_analyzer import DataAnalyzer
import streamlit as st


def initialize_state():
    if "query_button_clicked" not in st.session_state:
        st.session_state.query_button_clicked = False


def choose_dataframe():
    options = ['', 'L1, L2, L3 values', 'Total values']
    selected_option = st.radio('Select Dataframe to analyze', list(options))
    return selected_option


def show_options():
    options = ['', 'List columns', 'Sample', 'Describe', 'SQL query']
    selected_option = st.radio('Select Action', list(options))
    return selected_option



# Function to read large parquet file
def scan_large_parquet(file):
    df_scan = pl.read_parquet(file)
    return df_scan


st.set_page_config(layout="wide")
initialize_state()

st.write('<h3>Place your parquet file here</h3>', unsafe_allow_html=True)

# File uploader for parquet file
uploaded_file = st.file_uploader("Choose a file", type=["parquet"])

if uploaded_file is not None:
    # Display file details
    st.write("Filename:", uploaded_file.name)
    st.write("File size:", uploaded_file.size, "bytes")

    # Read and process the file
    with st.spinner('Reading the file...'):
        df_all = scan_large_parquet(uploaded_file)

    st.success('File successfully read!')

    # Create df_L1_L2_L3 and df_total dataframes
    total_columns = ['Total current', 'Total active power', 'Total apparent power',
                     'Total active energy', 'Total active returned energy',
                     'L1 total active energy', 'L1 total active returned energy',
                     'L2 total active energy', 'L3 total active returned energy',
                     'L3 total active energy', 'L3 total active returned energy_right']
    df_L1_L2_L3 = df_all.drop(total_columns)
    df_L1_L2_L3 = df_L1_L2_L3.rename({'L1 current': 'l1_current', 'L1 voltage': 'l1_voltage',
                                      'L1 active power': 'l1_active_power', 'L1 apparent power': 'l1_apparent_power',
                                      'L1 Power factor': 'l1_power_factor', 'L1 frequency': 'l1_frequency',
                                      'L2 current': 'l2_current', 'L2 voltage': 'l2_voltage',
                                      'L2 active power': 'l2_active_power', 'L2 apparent power': 'l2_apparent_power',
                                      'L2 Power factor': 'l2_power_factor', 'L2 frequency': 'l2_frequency',
                                      'L3 current': 'l3_current', 'L3 voltage': 'l3_voltage',
                                      'L3 active power': 'l3_active_power', 'L3 apparent power': 'l3_apparent_power',
                                      'L3 Power factor': 'l3_power_factor', 'L3 frequency': 'l3_frequency'})
    data_columns = ['L1 current', 'L1 voltage', 'L1 active power', 'L1 apparent power', 'L1 Power factor',
                    'L1 frequency',
                    'L2 current', 'L2 voltage', 'L2 active power', 'L2 apparent power', 'L2 Power factor',
                    'L2 frequency',
                    'L3 current', 'L3 voltage', 'L3 active power', 'L3 apparent power', 'L3 Power factor',
                    'L3 frequency']
    df_total = df_all.drop(data_columns)
    df_total = df_total.rename({'L1 total active energy': 'l1_total_active_energy',
                                'L1 total active returned energy': 'l1_total_active_returned_energy',
                                'L2 total active energy': 'l2_total_active_energy',
                                'L3 total active returned energy': 'l3_total_active_returned_energy',
                                'L3 total active energy': 'l3_total_active_energy',
                                'L3 total active returned energy_right': 'l3_total_active_returned_energy_right',
                                'Total current': 'total_current',
                                'Total active power': 'total_active_power',
                                'Total apparent power': 'total_apparent_power',
                                'Total active energy': 'total_active_energy',
                                'Total active returned energy': 'total_active_returned_energy'})

    st.write('<h3>L1, L2, L3 values:</h3>', unsafe_allow_html=True)
    st.write(df_L1_L2_L3.head())
    st.write('<h3>Total values:</h3>', unsafe_allow_html=True)
    st.write(df_total.head())

    chosen_dataframe = choose_dataframe()

    df = None
    df_type = ''
    if chosen_dataframe == 'L1, L2, L3 values':
        df = df_L1_L2_L3
        df_type = 'L'
    if chosen_dataframe == 'Total values':
        df = df_total
        df_type = 'Total'

    if df is not None:
        analyzer = DataAnalyzer(df, df_type)

        action = show_options()

        if action != 'SQL query':
            st.session_state.query_button_clicked = False
        if action == 'List columns':
            st.write('<h3>Columns in the DataFrame:</h3>', unsafe_allow_html=True)
            analyzer.list_columns()
        if action == 'Sample':
            st.write('<h3>Sample from the DataFrame:</h3>', unsafe_allow_html=True)
            st.write(analyzer.show_sample())
        if action == 'Describe':
            st.write("<h3>Summary statistics of the DataFrame:</h3>", unsafe_allow_html=True)
            st.write(analyzer.describe_dataframe())
        if action == 'SQL query':
            analyzer.query_with_sql()
