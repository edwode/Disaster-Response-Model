import sys
import pandas as pd
from general_utils.utils import *


class LoadData:
    def __init__(self, filename_mess, filename_cat):
        self.filename_mess = filename_mess
        self.filename_cat = filename_cat

    def load_data_messages(self):
        messages = ReadWriteS3.create_connection().read_from_s3(filename=self.filename_mess)
        return messages

    def load_data_categorical(self):
        categories = ReadWriteS3.create_connection().read_from_s3(filename=self.filename_cat)

        return categories

    def merge_data(self):
        messages = self.load_data_messages()
        categories = self.load_data_categorical()

        df = messages.merge(categories, on='id')

        return df


def clean_data(df):
    """
    - Cleans the combined dataframe for use by ML model

    Args:
    df pandas_dataframe: Merged dataframe returned from load_data() function

    Returns:
    df pandas_dataframe: Cleaned data to be used by ML model
    """

    # Split categories into separate category columns
    categories = df['categories'].str.split(";", expand=True)

    # select the first row of the categories dataframe
    row = categories.iloc[0, :].values

    # use this row to extract a list of new column names for categories.
    new_cols = [r[:-2] for r in row]

    # rename the columns of `categories`
    categories.columns = new_cols

    # Convert category values to just numbers 0 or 1.
    for column in categories:
        # set each value to be the last character of the string
        categories[column] = categories[column].str[-1]

        # convert column from string to numeric
        categories[column] = pd.to_numeric(categories[column])

    # drop the original categories column from `df`
    df.drop('categories', axis=1, inplace=True)

    # concatenate the original dataframe with the new `categories` dataframe
    df[categories.columns] = categories

    # drop duplicates
    df.drop_duplicates(inplace=True)

    return df


class DataSet:
    pass