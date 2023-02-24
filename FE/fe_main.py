from utils.cloud_utils import *
from utils import transformation


def main():
    parser = argparse.ArgumentParser()

    # creating two variables using the add_argument method
    parser.add_argument("num1", help="first number")
    parser.add_argument("num2", help="second number")
    parser.add_argument("operation", help="operation")

    args = parser.parse_args()

   # read the message csv from s3 and store as a dataframe

    s3c = boto3.client("s3", aws_access_key_id=get_configuration("aws_access_key_id"),
                     aws_secret_access_key=get_configuration("aws_secret_access_key"))

    obj = s3c.get_object(Bucket=disaster_messages.csv, Key=KEY)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')

    # read categories csv and store as a dataframe

    s3c = boto3.client("s3", aws_access_key_id=get_configuration("aws_access_key_id"),
                     aws_secret_access_key=get_configuration("aws_secret_access_key"))

    obj = s3c.get_object(Bucket=disaster_categories.csv, Key=KEY)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')

    df = messages_df.merge(categories_df, on='id')
    # read from the database and store as a dataframe



        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
    df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
       df = transformation.clean_data(df)

        # write a function to save data to s3
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)

        print('Cleaned data saved to database!')

    else:
        print('Please provide the filepaths of the messages and categories ' \
              'datasets as the first and second argument respectively, as ' \
              'well as the filepath of the database to save the cleaned data ' \
              'to as the third argument. \n\nExample: python process_data.py ' \
              'disaster_messages.csv disaster_categories.csv ' \
              'DisasterResponse.db')


import argparse



print(args.num1)