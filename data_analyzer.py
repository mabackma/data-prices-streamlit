import polars as pl

class DataAnalyzer:
    def __init__(self, dataframe):
        self.dataframe = dataframe


    def list_columns(self):
        columns = ''
        for column in self.dataframe.columns:
            columns += f'    {column}\n'
        return columns


    def show_sample(self):
        return self.dataframe.sample(5)


    def describe_dataframe(self):
        return self.dataframe.describe()


    def query_with_sql(self):
        query = input("\nEnter the SQL query: ")
        result = self.dataframe.sql(query)

        print("\nResult of SQL query:")
        print(result)