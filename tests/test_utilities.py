import pandas as pd
import os

from src.utilities import df_to_csv


class TestUtilities:
    def setup_method(self, method):
        # Define the DataFrame
        data = {'Name': ['John', 'Alice', 'Bob'], 'Age': [25, 30, 35], 'City': ['New York', 'London', 'Paris']}
        self.df = pd.DataFrame(data)

    def test_df_to_csv_file_created(self):
        # Call the function to be tested
        df_to_csv(self.df, "output")
        expected_file_path = os.path.join(os.getcwd(), r"data\clean_data", "output.csv")

        assert os.path.isfile(expected_file_path), "CSV file was not created"

    def test_df_to_csv_data_match(self):
        # Call the function to be tested
        df_to_csv(self.df, "output")
        file_path = os.path.join(os.getcwd(), r"data\clean_data", "output.csv")
        df_read = pd.read_csv(file_path)

        assert self.df.equals(df_read), "DataFrame content does not match the CSV file"

        # Clean up: remove the created CSV file
        os.remove(file_path)