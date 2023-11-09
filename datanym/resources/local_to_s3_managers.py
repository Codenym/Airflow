from dagster import IOManager, OutputContext, InputContext
import pickle
from .utils import get_file_path, get_file_name
import boto3
import csv
import pandas as pd
from typing import Union


class LocalPickleToS3CSVIOManager(IOManager):
    """
    An IOManager that handles the transfer of data from a local system in a pickle format to an AWS S3 bucket in CSV format.

    :param local_directory_path: The local directory path where the data is stored initially.
    :param s3_bucket: The name of the S3 bucket to upload the data to.
    :param s3_directory: The directory within the S3 bucket to store the data.
    """

    def __init__(self, local_directory_path: str, s3_bucket: str, s3_directory: str):
        self.local_directory_path = local_directory_path
        self.s3_bucket = s3_bucket
        self.s3_directory = s3_directory.rstrip('/')

    def handle_output(self, context: OutputContext, obj: Union[pd.DataFrame, list[dict], tuple[dict]]):
        """
        Handles the output data from Dagster computation, saving it locally as a CSV file and then uploading it to an S3 bucket.

        :param context: The output context from Dagster, containing metadata and configuration.
        :param obj: The object to be handled, which can be a pandas DataFrame or any object that can be written as rows in a CSV file.
        """
        local_file_path = f"{get_file_path(context, self.local_directory_path)}.csv"

        if isinstance(obj, pd.DataFrame):
            obj.to_csv(local_file_path)

        if isinstance(obj, (list, tuple)) and isinstance(obj[0], dict):
            with open(local_file_path, 'w', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, obj[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(obj)

        session = boto3.Session(profile_name='codenym')
        s3 = session.client('s3')

        s3_key = f"{self.s3_directory}/{get_file_name(context)}.csv"

        s3.upload_file(Filename=local_file_path,
                       Bucket=self.s3_bucket,
                       Key=s3_key)

        context.add_output_metadata(
            metadata={
                "output_location": f"s3://{self.s3_bucket}/{s3_key}"
            }
        )

    def load_input(self, context: InputContext) -> any:
        """
        Loads input data for a Dagster computation, reading from a local pickle file.

        :param context: The input context from Dagster, containing metadata and configuration.
        :return: The object loaded from the pickle file.
        """
        with open(get_file_path(context, self.local_directory_path), "rb") as handle:
            return pickle.load(handle)
