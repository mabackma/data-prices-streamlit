from datetime import datetime, timedelta
import polars as pl
from data_analyzer import DataAnalyzer
import streamlit as st
from dictionaries import location_names


def initialize_state():
    if "query_button_clicked" not in st.session_state:
        st.session_state.query_button_clicked = False
    if "line_chart_button_clicked" not in st.session_state:
        st.session_state.line_chart_button_clicked = False
    if "heatmap_button_clicked" not in st.session_state:
        st.session_state.heatmap_button_clicked = False
    if "profitability_button_clicked" not in st.session_state:
        st.session_state.profitability_button_clicked = False
    if "analyzer_L" not in st.session_state:
        st.session_state.analyzer_L = None
    if "analyzer_total" not in st.session_state:
        st.session_state.analyzer_total = None
    if "locations" not in st.session_state:
        st.session_state.locations = []


def choose_dataframe():
    options = ['', 'L1, L2, L3 values', 'Total values']
    selected_option = st.radio('Select Dataframe to analyze', list(options))
    return selected_option


def show_options():
    options = ['', 'List columns', 'Sample', 'Describe', 'SQL query', 'Line chart', 'Heatmap', 'Profitability',
               'Cost-effectiveness']
    selected_option = st.radio('Select Action', list(options))
    return selected_option


def choose_location():
    location_display_names = [location_names[key] for key in st.session_state.locations]
    selected_display_name = st.selectbox('Select Location', location_display_names)

    # Create a dictionary to map display names back to keys
    display_name_to_key = {v: k for k, v in location_names.items()}
    selected_location = display_name_to_key[selected_display_name]
    return selected_location


def get_dates_for_week(year, week_number):
    # Get the first day of the year
    first_day = datetime(year, 1, 1)

    # Calculate the start of the first week
    start_of_first_week = first_day - timedelta(days=first_day.isocalendar()[2] - 1)

    # Calculate the start date of the selected week
    start_date = start_of_first_week + timedelta(weeks=week_number - 1)

    # Calculate the end date of the selected week
    end_date = start_date + timedelta(days=7)

    return start_date.date(), end_date.date()


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

    end_date = end_date + timedelta(days=1)
    return start_date.date(), end_date.date()


def choose_time_interval():
    year_choices = ['2023', '2024']
    time_intervals = ['day', 'week', 'month']

    columns = st.columns(3)
    with columns[0]:
        time_interval = st.radio('Select time interval', time_intervals)

    start = None
    end = None
    with columns[1]:
        if time_interval == 'day':
            start = st.date_input("Select day", datetime.now())
            end = start + timedelta(days=1)
        if time_interval == 'week':
            year = st.radio('Select year', year_choices, index=1)
            year = int(year)
            with columns[2]:
                week_number = st.number_input('Select week number', value=1, min_value=1, max_value=52)
                start, end = get_dates_for_week(year, week_number)
        if time_interval == 'month':
            year = st.radio('Select year', year_choices, index=1)
            year = int(year)
            with columns[2]:
                month = st.number_input('Select month', value=1, min_value=1, max_value=12)
                start, end = get_dates_for_month(year, month)

    st.write("Start date:", start)
    st.write("End date:", end)
    return start, end


# Function to read large parquet file
@st.cache_data
def scan_large_parquet(file):
    df_scan = pl.read_parquet(file)
    return df_scan


st.set_page_config(layout="wide")
initialize_state()

if st.session_state.analyzer_L is None and st.session_state.analyzer_total is None:
    # File uploader for parquet file
    st.write('<h3>Place your parquet file here</h3>', unsafe_allow_html=True)
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
            elif 'ts' not in col and 'meter_id' not in col and 'price' not in col:
                data_columns.append(col)

        # Create df_L1_L2_L3 and df_total dataframes
        df_L1_L2_L3 = df_all.drop(total_columns)
        df_L1_L2_L3 = df_L1_L2_L3.rename({col: col.replace(' ', '_').lower() for col in df_L1_L2_L3.columns})
        df_total = df_all.drop(data_columns)
        df_total = df_total.rename({col: col.replace(' ', '_').lower() for col in df_total.columns})

        # Prices are in EUR / MWh and total_active_power is in W, so divide by 1 000 000 to get EUR / h
        df_total = df_total.with_columns(
            ((pl.col('total_active_power') * pl.col('price'))/1000000).alias('profitability'))
        # Price to active power ratio column
        df_total = df_total.with_columns(
            ((pl.col('price')) / pl.col('total_active_power')).alias('price_power_ratio'))

        st.session_state.analyzer_L = DataAnalyzer(df_L1_L2_L3, 'L')
        st.session_state.analyzer_total = DataAnalyzer(df_total, 'Total')

        st.write('<h3>L1, L2, L3 values:</h3>', unsafe_allow_html=True)
        st.write(df_L1_L2_L3.head())
        st.write('<h3>Total values:</h3>', unsafe_allow_html=True)
        st.write(df_total.head())

        st.session_state.locations = df_all.select(pl.col('meter_id')).unique()['meter_id'].to_list()
        chosen_dataframe = choose_dataframe()
else:
    chosen_dataframe = choose_dataframe()

    analyzer = None
    if chosen_dataframe == 'L1, L2, L3 values':
        analyzer = st.session_state.analyzer_L
    if chosen_dataframe == 'Total values':
        analyzer = st.session_state.analyzer_total

    if analyzer is not None:
        action = show_options()

        if action != 'SQL query':
            st.session_state.query_button_clicked = False
        if action != 'Line chart':
            st.session_state.line_chart_button_clicked = False
        if action != 'Heatmap':
            st.session_state.heatmap_button_clicked = False
        if action != 'Profitability':
            st.session_state.profitability_button_clicked = False
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
        if action == 'Line chart':
            location = choose_location()
            start_time, end_time = choose_time_interval()
            if location is not None:
                analyzer.line_chart(location, start_time, end_time)
        if action == 'Heatmap':
            location = choose_location()
            start_time, end_time = choose_time_interval()
            if location is not None:
                analyzer.draw_heatmaps(location, start_time, end_time)
        if action == 'Profitability':
            if chosen_dataframe == 'L1, L2, L3 values':
                st.write('Profitability not available for this dataframe')
            else:
                start_time, end_time = choose_time_interval()
                analyzer.profitability_line_chart(start_time, end_time)
        if action == 'Cost-effectiveness':
            if chosen_dataframe == 'L1, L2, L3 values':
                st.write('Cost-effectiveness not available for this dataframe')
            else:
                start_time, end_time = choose_time_interval()
                analyzer.cost_effectiveness(start_time, end_time)
