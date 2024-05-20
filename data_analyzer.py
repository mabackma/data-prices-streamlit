import pathlib
from datetime import datetime
import polars as pl
import pandas as pd
import streamlit as st
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime, timedelta


def callback_query():
    st.session_state.query_button_clicked = True


def callback_lines():
    st.session_state.line_button_clicked = True


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


def get_dates_for_week(year, week_number):
    # Get the first day of the year
    first_day = datetime(year, 1, 1)

    # Calculate the start of the first week
    start_of_first_week = first_day - timedelta(days=first_day.isocalendar()[2] - 1)

    # Calculate the start date of the selected week
    start_date = start_of_first_week + timedelta(weeks=week_number - 1)

    # Calculate the end date of the selected week
    end_date = start_date + timedelta(days=7)

    return start_date, end_date


def get_dates_for_month(year, month_number):
    # Calculate the start date of the month
    start_date = datetime(year, month_number, 1)

    # Calculate the end date of the month
    if month_number == 2:
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            end_date = datetime(year, month_number, 29)  # Leap year
        else:
            end_date = datetime(year, month_number, 28)  # Non-leap year
    elif month_number in [4, 6, 9, 11]:
        end_date = datetime(year, month_number, 30)
    else:
        end_date = datetime(year, month_number, 31)

    return start_date, end_date


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

    def line_chart(self, location):
        query_string = f"SELECT * FROM self WHERE meter_id = '{location}'"
        location_df = self.dataframe.sql(query_string)

        year_choices = ['2023', '2024']
        time_intervals = ['day', 'week', 'month']

        time_interval = st.radio('Select time interval', time_intervals)

        start = None
        end = None
        if time_interval == 'day':
            start = st.date_input("Select day", datetime.now())
            end = start
        if time_interval == 'week':
            year = st.radio('Select year', year_choices)
            year = int(year)
            week_number = st.number_input('Select week number', value=1, min_value=1, max_value=52)
            start, end = get_dates_for_week(year, week_number)
        if time_interval == 'month':
            year = st.radio('Select year', year_choices)
            year = int(year)
            month = st.number_input('Select month', value=1, min_value=1, max_value=12)
            start, end = get_dates_for_month(year, month)

        st.write("Start date:", start)
        st.write("End date:", end)

        location_df = location_df.filter((pl.col('ts') >= start) & (pl.col('ts') <= end))

        lines = []
        for column in location_df.columns:
            if column != 'ts' and column != 'meter_id':
                if st.checkbox(column):
                    lines.append(column)

        # Convert to pandas before drawing line chart
        location_df = location_df.to_pandas()
        location_df['ts'] = pd.to_datetime(location_df['ts'])
        location_df.set_index('ts', inplace=True)

        if not st.session_state.line_button_clicked:
            st.button('Click here to draw line charts', on_click=callback_lines)
        if st.session_state.line_button_clicked:
            st.write(f'<h3>Location: {location}</h3>', unsafe_allow_html=True)
            st.write(f'<h4>Time range: {start} - {end}</h4>', unsafe_allow_html=True)

            # Normalize selected columns
            scaler = MinMaxScaler()
            if len(location_df) > 0:
                # Fill None values with 0
                location_df[lines] = location_df[lines].fillna(0)
                location_df[lines] = scaler.fit_transform(location_df[lines])

            # Draw the line chart
            st.line_chart(location_df[lines])
