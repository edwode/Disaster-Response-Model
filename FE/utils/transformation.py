import pandas as pd


def clean_data(df):
    """

    :param df:
    :return:
    """
    """
    - Cleans the combined dataframe for use by ML model

    Args:
    df pandas_dataframe: Merged dataframe returned from load_data() function

    Returns:
    df pandas_dataframe: Cleaned data to be used by ML model
    """

    # Split categories into separate category columns
    categories = df['categories'].str.split(";", \
                                            expand=True)

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
