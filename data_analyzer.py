import pathlib
import polars as pl
import pandas as pd
import streamlit as st
from sklearn.preprocessing import MinMaxScaler


def callback_query():
    st.session_state.query_button_clicked = True


def callback_lines():
    st.session_state.line_chart_button_clicked = True


# Replace any characters that are not allowed in a filename
def sanitize_filename(filename):
    sanitized_filename = ''
    for letter in filename:
        sanitized_filename += switch(letter)
    return sanitized_filename


def switch(symbol):
    if symbol == "<":
        return "LESS_THAN"
    if symbol == ">":
        return "GREATER_THAN"
    if symbol == ":":
        return "COLON"
    if symbol == ";":
        return ""
    if symbol == "\"":
        return "D_QUOTE"
    if symbol == "/":
        return "F_SLASH"
    if symbol == "\\":
        return "B_SLASH"
    if symbol == "|":
        return "OR"
    if symbol == "?":
        return "CONDITION"
    if symbol == "*":
        return "ALL"
    return symbol


def get_hourly_values(df):
    # Select only numeric columns
    numeric_cols = df.select_dtypes(include='number').columns
    numeric_df = df[numeric_cols]

    # Resample the data to hourly frequency and compute the mean for each hour
    hourly_df = numeric_df.resample('h').mean()
    return hourly_df


class DataAnalyzer:
    def __init__(self, dataframe, dataframe_type):
        self.dataframe = dataframe
        self.dataframe_type = dataframe_type

    def list_columns(self):
        for column in self.dataframe.columns:
            st.write(column)

    def show_sample(self):
        return self.dataframe.sample(5)

    def describe_dataframe(self):
        return self.dataframe.describe()

    def query_with_sql(self):
        query_string = st.text_input('Enter the SQL query:')

        if not st.session_state.query_button_clicked:
            st.button('Click here to see the results', on_click=callback_query)
        if st.session_state.query_button_clicked:
            result = self.dataframe.sql(query_string)

            dirpath = pathlib.Path('./query_files/')
            file_name = sanitize_filename(query_string.replace(' ', '_'))
            path = dirpath / f'{self.dataframe_type}_{file_name}.parquet'

            # Create the directory if it doesn't exist
            dirpath.mkdir(exist_ok=True)
            if not pathlib.Path(path).exists():
                result.write_parquet(path)

            st.write('<h3>Result of SQL query:</h3>', unsafe_allow_html=True)
            st.write(result.head())
            st.write(f"Number of rows: {result.height}")

    def line_chart(self, location, start, end):
        query_string = f"SELECT * FROM self WHERE meter_id = '{location}'"
        location_df = self.dataframe.sql(query_string)
        location_df = location_df.filter((pl.col('ts') >= start) & (pl.col('ts') < end))

        lines = []
        for column in location_df.columns:
            if column != 'ts' and column != 'meter_id':
                if st.checkbox(column):
                    lines.append(column)

        # Convert to pandas before drawing line chart
        location_df = location_df.to_pandas()
        location_df['ts'] = pd.to_datetime(location_df['ts'])
        location_df.set_index('ts', inplace=True, drop=False)

        if not st.session_state.line_chart_button_clicked:
            st.button('Click here to draw line charts', on_click=callback_lines)
        if st.session_state.line_chart_button_clicked:
            if len(location_df) > 0:
                # Normalize selected columns
                scaler = MinMaxScaler()
                if len(lines) > 0:
                    # Fill None values with 0
                    location_df[lines] = location_df[lines].fillna(0)
                    location_df[lines] = scaler.fit_transform(location_df[lines])

                    # Get hourly values
                    hourly_df = get_hourly_values(location_df)

                    # Draw the line chart
                    st.write(f'<h3>Location: {location}</h3>', unsafe_allow_html=True)
                    st.write(f'<h4>Time range: {start} - {end}</h4>', unsafe_allow_html=True)
                    st.line_chart(hourly_df[lines])
                else:
                    st.write('Choose columns to draw line chart')
            else:
                st.write('Choose another time range')