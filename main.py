#!/usr/bin/env python

import click

from general_utils.utils import *


@click.option(
    "--is_fe",
    "-fe",
    "is_fe",
    required=True,
    type=click.Choice(["baseline"], case_sensitive=False),
    help="what type of run you want to perform (FE or Model"
)
def run_process(is_fe):
    if is_fe:
        read = ReadWriteS3.create_connection()
        df = read.read_from_s3(filename="disaster_categories.csv")

        print(df.head())

    else:
        pass


if __name__ == "__main__":
    run_process()