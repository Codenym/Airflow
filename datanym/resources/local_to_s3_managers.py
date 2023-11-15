from dagster import IOManager, OutputContext, InputContext, MetadataValue
import boto3
import csv
import pandas as pd
from typing import Union
from .output_metadata import (
    add_tuple_metadata,
    add_list_metadata,
    add_dataframe_metadata,
    add_path_metadata,
    add_metadata
)
from pathlib import Path


class LocalPickleToS3CSVIOManager(IOManager):
    """
    An IOManager that handles the transfer of data from a local system in a pickle format to an AWS S3 bucket in CSV format.

    :param local_directory_path: The local directory path where the data is stored initially.
    :param s3_bucket: The name of the S3 bucket to upload the data to.
    :param s3_directory: The directory within the S3 bucket to store the data.
    """

    def __init__(self, local_directory_path: Path, s3_bucket: str, s3_directory: str):
        self.local_directory_path = local_directory_path
        self.s3_bucket = s3_bucket
        self.s3_directory = s3_directory.rstrip('/')

    def handle_output(self, context: OutputContext, obj: Union[Path, pd.DataFrame, list[dict], tuple[dict]]):
        """
        Handles the output data from Dagster computation, saving it locally as a CSV file and then uploading it to an S3 bucket.

        :param context: The output context from Dagster, containing metadata and configuration.
        :param obj: The object to be handled, which can be a pandas DataFrame or any object that can be written as rows in a CSV file.
        """
        s3_key = f"{self.s3_directory}/{context.asset_key.path[-1]}.csv"
        target_s3_path = f"s3://{self.s3_bucket}/{s3_key}"
        add_metadata(context=context, metadata={"target s3 path": MetadataValue.text(target_s3_path)})

        if isinstance(obj, pd.DataFrame):
            obj.to_csv(target_s3_path)
            add_dataframe_metadata(context=context, obj=obj)

        elif isinstance(obj, (list, tuple)) and isinstance(obj[0], dict):
            # Write the data to a local CSV file
            local_file_path = Path(f"{self.local_directory_path / context.asset_key.path[-1]}.csv")
            add_metadata(context=context, metadata={"local origin file path": MetadataValue.path(local_file_path)})

            with open(local_file_path, 'w', newline='') as output_file:
                dict_writer = csv.DictWriter(f=output_file, fieldnames=obj[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(obj)

        elif isinstance(obj, Path):
            local_file_path = obj

        else:
            raise ValueError(f"Add logic to convert type to CSV file to LocalPickleToS3CSVIOManager/handle_output")

        if not isinstance(obj, pd.DataFrame):
            # Upload the local CSV file to S3
            session = boto3.Session(profile_name='codenym')
            s3 = session.client('s3')
            s3.upload_file(Filename=local_file_path,
                           Bucket=self.s3_bucket,
                           Key=s3_key)
            df = pd.read_csv(local_file_path, nrows=10).to_markdown()
            add_metadata(context, metadata={'csv head': MetadataValue.md(df)})

        if isinstance(obj, list):            add_list_metadata(context=context, obj=obj)
        elif isinstance(obj, tuple):         add_tuple_metadata(context=context, obj=obj)
        elif isinstance(obj, pd.DataFrame):  add_dataframe_metadata(context=context, obj=obj)
        elif isinstance(obj, Path):          add_path_metadata(context=context, obj=obj)
        else: raise ValueError(f"Unsupported type: {type(obj)}.  Add type LocalPickleToS3CSVIOManager/handle_output")

    def load_input(self, context: InputContext) -> any:
        """
        Loads input data for a Dagster computation, reading from a local pickle file.

        :param context: The input context from Dagster, containing metadata and configuration.
        :return: The object loaded from the pickle file.
        """
        s3_key = f"{self.s3_directory}/{context.asset_key.path[-1]}.csv"
        target_s3_path = f"s3://{self.s3_bucket}/{s3_key}"
        return target_s3_path
