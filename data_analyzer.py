import pathlib
import streamlit as st


def callback_query():
    st.session_state.query_button_clicked = True


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