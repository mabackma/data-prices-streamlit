import pathlib
import polars as pl
import pandas as pd
import numpy as np
import streamlit as st
from sklearn.preprocessing import MinMaxScaler
import plotly.express as px
from dictionaries import location_names, units
import time
import os

def callback_query():
    st.session_state.query_button_clicked = True


def callback_lines():
    st.session_state.line_chart_button_clicked = True


def callback_heatmap():
    st.session_state.heatmap_button_clicked = True


def callback_expenses():
    st.session_state.expenses_button_clicked = True


# Function to remove files older than a certain age
def remove_old_files(directory, age_in_seconds):
    now = time.time()
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path) and now - os.path.getmtime(file_path) > age_in_seconds:
            os.remove(file_path)


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


def to_helsinki_time(df):
    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'])
        if df['ts'].iloc[0].month < 6:
            df['ts'] = df['ts'].dt.tz_localize('Europe/Helsinki', nonexistent='shift_forward')
        else:
            df['ts'] = df['ts'].dt.tz_localize('Europe/Helsinki', nonexistent='shift_backward')
        df.set_index('ts', inplace=True)
    return df


def get_hourly_values(df):
    # Select only numeric columns
    numeric_cols = df.select_dtypes(include='number').columns
    numeric_df = df[numeric_cols]

    # Resample the data to hourly frequency and compute the mean
    hourly_df = numeric_df.resample('h').mean()
    return hourly_df


def get_hourly_values_fill_none(df):
    # Select only numeric columns
    numeric_cols = df.select_dtypes(include='number').columns
    numeric_df = df[numeric_cols]

    # Resample the data to hourly frequency and compute the mean
    hourly_df = numeric_df.resample('h').mean()

    # Fill null values with the mean
    hourly_df[numeric_cols] = hourly_df[numeric_cols].fillna(hourly_df[numeric_cols].mean())
    return hourly_df


def draw_heatmap(data, sensor):
    # Convert index to string for proper display in Plotly
    data.index = data.index.astype(str)

    # Create Plotly heatmap
    fig = px.imshow(data.T, labels=dict(x="Date", y="Hour of Day", color=f"{units[sensor]}"))
    fig.update_layout(
        title=f'{sensor}',
        xaxis_title='Date',
        yaxis_title='Hour of Day',
        width=400,
        height=500
    )

    # Display the Plotly heatmap in Streamlit
    st.plotly_chart(fig, theme="streamlit")


class DataAnalyzer:
    def __init__(self, dataframe, dataframe_type):
        self.dataframe = dataframe
        self.dataframe_type = dataframe_type
        self.query_dir = pathlib.Path('./query_files/')
        self.cleanup_interval = 3600  # 1 hour

    def list_columns(self):
        for column in self.dataframe.columns:
            st.write(column)

    def show_sample(self):
        return self.dataframe.sample(5)

    def describe_dataframe(self):
        return self.dataframe.describe()

    # Makes a query to the dataframe and saves the result in a parquet file
    # The filename is the query string
    def query_with_sql(self):
        # Cleanup old files every time the query is run
        remove_old_files(self.query_dir, self.cleanup_interval)

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

            # Load the data from the parquet file for downloading
            with open(path, "rb") as f:
                data = f.read()

            # Create a download button
            st.download_button(
                label="Download result as Parquet file",
                data=data,
                file_name=path.name,
                mime="application/octet-stream"
            )

    # Returns the dataframe for a chosen meter_id and its chosen sensors
    def prepare_dataframe(self, location, start, end):
        query_string = f"SELECT * FROM self WHERE meter_id = '{location}'"
        location_df = self.dataframe.sql(query_string)
        location_df = location_df.filter((pl.col('ts') >= start) & (pl.col('ts') < end))

        number_of_columns = 2 if len(location_df.columns) < 15 else 6
        cols = st.columns(number_of_columns)
        sensors = []
        for i, sensor in enumerate(location_df.columns, start=-2):
            with cols[i % number_of_columns]:
                if sensor != 'ts' and sensor != 'meter_id' and sensor != 'expenses' and sensor != 'power_to_price_ratio':
                    if st.checkbox(sensor):
                        sensors.append(sensor)

        # Convert to pandas before drawing line chart or heatmap
        location_df = location_df.to_pandas()
        location_df['ts'] = pd.to_datetime(location_df['ts'])
        location_df.set_index('ts', inplace=True, drop=False)
        return sensors, location_df

    # Draws line charts for chosen sensors
    def line_chart(self, location, start, end):
        lines, location_df = self.prepare_dataframe(location, start, end)

        if not st.session_state.line_chart_button_clicked:
            st.button('Click here to draw line charts', on_click=callback_lines)
        if st.session_state.line_chart_button_clicked:
            if len(location_df) > 0:
                # Normalize selected columns
                scaler = MinMaxScaler()
                if len(lines) > 0:
                    location_df[lines] = scaler.fit_transform(location_df[lines])
                    location_df = to_helsinki_time(location_df)

                    st.write(f'<h2>{location_names[location]}</h2>', unsafe_allow_html=True)
                    st.write(f'<h4>meter_id: {location}</h4>', unsafe_allow_html=True)
                    st.write(f'<h4>Time range: {start} - {end}</h4>', unsafe_allow_html=True)

                    # When checked, None values are set to 0
                    if st.checkbox("Hide interruptions"):
                        hourly_df = get_hourly_values_fill_none(location_df)
                    else:
                        hourly_df = get_hourly_values(location_df)

                    st.line_chart(hourly_df[lines])
                else:
                    st.write('Choose columns to draw line chart')
            else:
                st.write('Choose another time range')

    # Draws heatmaps for chosen sensors
    def draw_heatmaps(self, location, start, end):
        sensors, location_df = self.prepare_dataframe(location, start, end)

        if not st.session_state.heatmap_button_clicked:
            st.button('Click here to draw heatmap', on_click=callback_heatmap)
        if st.session_state.heatmap_button_clicked:
            if len(location_df) > 0:
                if len(sensors) > 0:
                    #location_df[sensors] = location_df[sensors].fillna(0)

                    # Prepare data for heatmap
                    location_df['hour'] = location_df.index.hour
                    location_df['day'] = location_df.index.date
                    location_df = to_helsinki_time(location_df)

                    st.write(f'<h2>{location_names[location]}</h2>', unsafe_allow_html=True)
                    st.write(f'<h4>meter_id: {location}</h4>', unsafe_allow_html=True)
                    st.write(f'<h4>Time range: {start} - {end}</h4>', unsafe_allow_html=True)

                    # Draw heatmaps in columns
                    columns = st.columns(4)
                    for i, sensor in enumerate(sensors, start=0):
                        heatmap_data = location_df.pivot_table(index='day', columns='hour', values=sensor,
                                                               aggfunc='mean')
                        with columns[i % 4]:
                            draw_heatmap(heatmap_data, sensor)
                else:
                    st.write('Choose columns to draw heatmap')
            else:
                st.write('Choose another time range')

    # Creates a pivot table with the chosen meter_id's as columns and expenses as their values.
    def prepare_expenses_df(self, start, end):
        query_string = f"""
            SELECT ts, meter_id, expenses
            FROM self
            WHERE ts >= '{start}' AND ts < '{end}'
        """
        expenses_df = self.dataframe.sql(query_string)

        cols = st.columns(5)
        locations = []
        for i, location in enumerate(st.session_state.locations, start=0):
            with cols[i % 5]:
                if st.checkbox(location_names[location]):
                    locations.append(location)

        # Filter the dataframe for the selected locations
        if locations:
            expenses_df = expenses_df.filter(pl.col('meter_id').is_in(locations))
        else:
            expenses_df = pl.DataFrame()

        # Convert to pandas before drawing line chart
        expenses_df = expenses_df.to_pandas()

        if not expenses_df.empty:
            # Pivot the dataframe to have a column for each location's expenses
            expenses_pivot_df = expenses_df.pivot_table(index='ts', columns='meter_id',
                                                                  values='expenses')
            # Rename the columns to include the location names
            expenses_pivot_df.rename(columns=location_names, inplace=True)
        else:
            expenses_pivot_df = pd.DataFrame()

        return expenses_pivot_df

    # Plots expenses (power * price) for individual meter_id and plots their total expenses.
    # Calculates the total cost for the chosen meters
    def expenses_line_chart(self, start, end):
        expenses_df = self.prepare_expenses_df(start, end)

        if not st.session_state.expenses_button_clicked:
            st.button('Click here to see expenses line charts', on_click=callback_expenses)
        if st.session_state.expenses_button_clicked:
            # Check if there are any selected locations
            if expenses_df.empty:
                st.write("No data available for the selected time range.")
                return
            else:
                lines = [col for col in expenses_df.columns if col != 'ts']
                if len(lines) > 0:
                    # Keep only positive values for the expenses
                    #expenses_df[lines] = expenses_df[lines].where(expenses_df[lines] >= 0)

                    # Get hourly values
                    hourly_df = get_hourly_values(expenses_df)

                    # Add column for total expenses
                    hourly_df['total_expenses'] = hourly_df[lines].sum(axis=1, skipna=True)
                    hourly_df = to_helsinki_time(hourly_df)

                    # Calculate the total cost
                    cost = hourly_df['total_expenses'].sum()

                    # Draw the line chart
                    st.write(f'<h3>Net expenses (€/h)</h3', unsafe_allow_html=True)
                    st.line_chart(hourly_df[lines])
                    st.write(f'<h3>Total net expenses (€/h)</h3>', unsafe_allow_html=True)
                    st.line_chart(hourly_df['total_expenses'])
                    st.write(f'<h4>Total cost of electricity during {start} - {end}:</h4>', unsafe_allow_html=True)
                    st.write(f'<h4>{cost:.2f} €</h4>', unsafe_allow_html=True)
                else:
                    st.write('Choose columns to draw line chart')

    # Plots Cost-effectiveness (power / price) and expenses (power * price)
    # Calculates the total cost for all meters
    def cost_effectiveness(self, start, end):
        # Expenses dataframe (power * price)
        cost_df = self.dataframe.filter((pl.col('ts') >= start) & (pl.col('ts') < end)).to_pandas()
        cost_df['ts'] = pd.to_datetime(cost_df['ts'])
        cost_df.set_index('ts', inplace=True)

        if cost_df.empty:
            st.write("No data available for the selected time range.")
            return
        else:
            # Pivot the dataframe to have a column for each location's expenses
            cost_pivot_df = cost_df.pivot_table(index='ts', columns='meter_id', values='expenses')

            lines = [col for col in cost_pivot_df.columns if col != 'ts']

            # Keep only negative values for the profits
            profitability_df = cost_pivot_df[lines].where(cost_pivot_df[lines] < 0)
            profitability_hourly_df = get_hourly_values(profitability_df)
            profitability_hourly_df['total_profit'] = profitability_hourly_df[lines].sum(axis=1, skipna=True)
            profitability_hourly_df['total_profit'] = profitability_hourly_df['total_profit'] * (-1)
            profit = profitability_hourly_df['total_profit'].sum()  # Total expenses

            cost_hourly_df = get_hourly_values(cost_pivot_df)
            cost_hourly_df['total_expenses'] = cost_hourly_df[lines].sum(axis=1, skipna=True)
            real_cost = cost_hourly_df['total_expenses'].sum()  # Total expenses

            # Keep only positive values for the expenses
            cost_pivot_df[lines] = cost_pivot_df[lines].where(cost_pivot_df[lines] >= 0)
            cost_hourly_df = get_hourly_values(cost_pivot_df)
            cost_hourly_df['total_expenses'] = cost_hourly_df[lines].sum(axis=1, skipna=True)
            cost = cost_hourly_df['total_expenses'].sum()  # Total expenses

            # Create Cost-effectiveness dataframe (power / price)
            ratio_df = self.dataframe.filter((pl.col('ts') >= start) & (pl.col('ts') < end)).select(
                ['ts', 'total_active_power', 'price']).to_pandas()
            ratio_df['ts'] = pd.to_datetime(ratio_df['ts'])
            ratio_df.set_index('ts', inplace=True)

            # Get hourly values
            ratio_hourly_df = get_hourly_values(ratio_df)

            # Make column for Cost-effectiveness (power / price)
            ratio_hourly_df['power_to_price_ratio'] = ratio_hourly_df['total_active_power'] / ratio_hourly_df['price']

            # Merge all the dataframes on the timestamp column
            ratio_hourly_df = pd.merge(ratio_hourly_df, cost_hourly_df[['total_expenses']], left_index=True,
                                       right_index=True)
            ratio_hourly_df = pd.merge(ratio_hourly_df, profitability_hourly_df[['total_profit']], left_index=True,
                                       right_index=True)

            # Normalize the lines for line chart
            scaler = MinMaxScaler()
            normalized_lines = ['total_expenses', 'total_profit', 'power_to_price_ratio']
            ratio_hourly_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            ratio_hourly_df[normalized_lines] = ratio_hourly_df[normalized_lines].fillna(0)
            ratio_hourly_df[normalized_lines] = scaler.fit_transform(ratio_hourly_df[normalized_lines])
            ratio_hourly_df = to_helsinki_time(ratio_hourly_df)

            # Draw the line chart
            st.write(f'<h2>Profitability and Expenses</h2>', unsafe_allow_html=True)
            st.line_chart(ratio_hourly_df[normalized_lines])
            st.write(f'<h3>Overview of Electricity Expenses for {start} - {end}:</h3>', unsafe_allow_html=True)
            st.write(f'<h5>Total from Hourly Expenses: {cost:.2f} €</h5>', unsafe_allow_html=True)
            st.write(f'<h5>Total from Hourly Profit: {profit:.2f} €</h5>', unsafe_allow_html=True)
            st.write(f'<h5>Total Cost: {real_cost:.2f} €</h5>', unsafe_allow_html=True)
            st.write(f'<h5></h5>', unsafe_allow_html=True)
            st.write(f'<h5>Calculation:</h5>', unsafe_allow_html=True)
            st.write(f'<h5>{cost:.2f} - {profit:.2f} = {(cost - profit):.2f} ≈ {real_cost:.2f} €</h5>', unsafe_allow_html=True)