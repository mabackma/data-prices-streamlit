import streamlit as st


def callback_query():
    st.session_state.query_button_clicked = True


class DataAnalyzer:
    def __init__(self, dataframe):
        self.dataframe = dataframe


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

            st.write('<h3>Result of SQL query:</h3>', unsafe_allow_html=True)
            st.write(result.head())