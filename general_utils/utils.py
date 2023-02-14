from decouple import config
import boto3
from io import StringIO, BytesIO

import pandas as pd

from sqlalchemy import create_engine


def get_configuration(configuration):
    return config(configuration)


class ReadWriteS3:
    """
    This class is used to read/load data to s3 bucket
    """
    @classmethod
    def create_connection(cls):
        """

        :return:
        """
        s3_client = boto3.client("s3", aws_access_key_id=get_configuration("aws_access_key_id"),
                                 aws_secret_access_key=get_configuration("aws_secret_access_key"))

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

        file_type = filename.split(".")[-1] # get the file extension from the filename

        obj = self.client.get_object(Bucket=bucket_name, Key=key)

        if file_type in ["csv", "xlsx"]:
            body = obj['Body']
            string = body.read().decode('utf-8')
            if file_type == "csv":
                return pd.read_csv(StringIO(string))

            return pd.read_excel(BytesIO(string))

        elif file_type == "feather":
            pass

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