from dagster import IOManager, OutputContext, InputContext, AssetKey
import pandas as pd
from typing import Union
from .output_metadata import add_metadata
import sqlite3
import os
from .utils import get_file_name


class S3CSVtoSqliteIOManager(IOManager):
    def __init__(self, sqlite_db: str):
        self.sqlite_db = sqlite_db

    def handle_output(self, context: OutputContext, obj):  # Union[pd.DataFrame, list[dict], tuple[dict]]):
        con = sqlite3.connect(self.sqlite_db)
        os.environ["AWS_DEFAULT_PROFILE"] = "codenym"
        df = pd.read_csv(obj)
        tbl_name = get_file_name(context).replace('sqlite_', '')
        df.to_sql(tbl_name, con, if_exists='replace', index=False)

        add_metadata(context, obj, f"{self.sqlite_db}.{tbl_name}")

    def load_input(self, context: InputContext) -> any:
        tbl_name = get_file_name(context)[:-4].replace('sqlite_', '')
        return f"sqlite::{self.sqlite_db}::{tbl_name}"
