from dagster import IOManager, OutputContext, InputContext
import pandas as pd
from .output_metadata import add_metadata
import sqlite3
from .utils import get_file_name
from typing import Union
from abc import abstractmethod


class S3CSVtoSqlIOManagerBase(IOManager):
    def __init__(self, db_type: str, db_host: str, profile_name: str = 'codenym'):
        self.db_host = db_host
        self.db_type = db_type
        self.profile_name = profile_name

    @staticmethod
    def _get_table_name(context: Union[InputContext, OutputContext]):
        # Eventually add schema here
        return get_file_name(context)

    def load_input(self, context: InputContext) -> (str, str, str):
        return {'db_type': self.db_type,
                'db_host': self.db_host,
                'context_name': self._get_table_name(context)}

    def handle_output(self, context: OutputContext, obj: str):
        self._move_to_sql(context, obj)
        add_metadata(context, obj, f"{self.db_host}.{self._get_table_name(context)}")

    @abstractmethod
    def _connect(self):
        raise NotImplementedError

    @abstractmethod
    def _move_to_sql(self, context: OutputContext, obj: str):
        raise NotImplementedError


class S3CSVtoSqliteIOManager(S3CSVtoSqlIOManagerBase):

    def __init__(self, db_host: str, profile_name: str = 'codenym'):
        super().__init__(db_type='sqlite', db_host=db_host, profile_name=profile_name)

    def _connect(self):
        return sqlite3.connect(self.db_host)

    def _move_to_sql(self, context: OutputContext, obj: str):
        conn = self._connect()
        df = pd.read_csv(obj, storage_options=dict(profile=self.profile_name))
        df.to_sql(self._get_table_name(context), conn, if_exists='replace', index=False)
