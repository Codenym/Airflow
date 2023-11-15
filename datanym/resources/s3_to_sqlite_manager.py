from dagster import IOManager, InputContext, OutputContext, MetadataValue
import pandas as pd
from .output_metadata import add_metadata
import sqlite3
from typing import Union
from abc import abstractmethod
from contextlib import contextmanager


class S3CSVtoSqlIOManagerBase(IOManager):
    def __init__(self, db_type: str, db_host: str, profile_name: str = 'codenym'):
        self.db_host = db_host
        self.db_type = db_type
        self.profile_name = profile_name

    @staticmethod
    def _get_table_name(context: Union[InputContext, OutputContext]):
        # Eventually add schema here
        return context.asset_key.path[-1]

    def load_input(self, context: InputContext) -> (str, str, str):
        return {'db_type': self.db_type,
                'db_host': self.db_host,
                'table_name': self._get_table_name(context)}

    def handle_output(self, context: OutputContext, obj: str):
        self._move_to_sql(context, obj)
        metadata = {'upstream_s3_path': MetadataValue.text(obj),
                    'db_type': MetadataValue.text(self.db_type),
                    'db_host': MetadataValue.text(self.db_host),
                    'table_name': MetadataValue.text(self._get_table_name(context))
                    }
        add_metadata(context, metadata)

    @abstractmethod
    @contextmanager
    def _connection_context(self):
        raise NotImplementedError

    @abstractmethod
    def _move_to_sql(self, context: OutputContext, obj: str):
        raise NotImplementedError


class S3CSVtoSqliteIOManager(S3CSVtoSqlIOManagerBase):

    def __init__(self, db_host: str, profile_name: str = 'codenym'):
        super().__init__(db_type='sqlite', db_host=db_host, profile_name=profile_name)

    @contextmanager
    def _connection_context(self):
        conn = sqlite3.connect(self.db_host)
        try:
            yield conn
        finally:
            conn.commit()
            conn.close()

    def _move_to_sql(self, context: OutputContext, obj: str):
        df = pd.read_csv(obj, storage_options=dict(profile=self.profile_name), dtype=str)
        with self._connection_context() as conn:
            df.to_sql(self._get_table_name(context), conn, if_exists='replace', index=False,
                      dtype={col: 'TEXT' for col in df.columns})
