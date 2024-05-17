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
@st.cache_data
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

    total_columns = []
    data_columns = []
    for col in df_all.columns:
        if 'total' in col or 'Total' in col:
            total_columns.append(col)
        else:
            data_columns.append(col)

    # Create df_L1_L2_L3 and df_total dataframes
    df_L1_L2_L3 = df_all.drop(total_columns)
    df_L1_L2_L3 = df_L1_L2_L3.rename({col: col.replace(' ', '_').lower() for col in df_L1_L2_L3.columns})
    df_total = df_all.drop(data_columns)
    df_total = df_total.rename({col: col.replace(' ', '_').lower() for col in df_total.columns})

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
