import boto3
from io import StringIO

import pandas as pd

from sqlalchemy import create_engine


class ReadWriteS3:
    """

    """
    @classmethod
    def create_connection(cls):
        """

        :return:
        """
        s3_client = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key)

        return cls(client=s3_client)

    def __init__(self, client):
        """

        :param client:
        """
        self.client = client

    def read_from_s3(self,  filename, bucket_name="projectsmodels", env="dev", path="raw_datasets"):
        """
        This method is used to read csv file from aws s3 bucket
        :param filename: str -> the file to be read from s3 bucket
        :param bucket_name: str -> the s3 bucket name
        :param env: str -> dev or uat or prod
        :param path: str -> the path to the file in s3
        :return: pd.DataFrame
        """
        key = f"{env}/{path}/{filename}"

        file_type = filename.split(".")[-1]

        if file_type == "csv":
            csv_obj = self.client.get_object(Bucket=bucket_name, Key=key)
            body = csv_obj['Body']
            csv_string = body.read().decode('utf-8')

            df = pd.read_csv(StringIO(csv_string))

            return df

    def write_to_s3(self):
        pass


def save_data(df, database_file_name):
    """
    Saves cleaned data to an SQL database

    Args:
    df pandas_dataframe: Cleaned data returned from clean_data() function
    database_file_name str: File path of SQL Database into which the cleaned\
    data is to be saved

    Returns:
    None
    """

    engine = create_engine('sqlite:///{}'.format(database_file_name))
    db_file_name = database_file_name.split("/")[-1]  # extract file name from \
    # the file path
    table_name = db_file_name.split(".")[0]
    df.to_sql(table_name, engine, index=False, if_exists='replace')



if __name__ == "__main__":
    read = ReadWriteS3.create_connection()
    df = read.read_from_s3(filename="disaster_categories.csv")

    print(df.head())