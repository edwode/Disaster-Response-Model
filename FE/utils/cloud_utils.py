import boto3
from io import StringIO

import pandas as pd


class ReadWriteS3:
    @classmethod
    def create_connection(cls):
        s3_client = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key)

        return cls(client=s3_client)

    def __init__(self, client):
        self.client = client

    def read_from_s3(self,  filename, bucket_name="projectsmodels", env="dev", path="raw_datasets"):
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


if __name__ == "__main__":
    read = ReadWriteS3.create_connection()
    df = read.read_from_s3(filename="disaster_categories.csv")

    print(df.head())