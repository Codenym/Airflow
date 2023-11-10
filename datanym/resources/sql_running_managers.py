from dagster import IOManager, OutputContext, InputContext, MetadataValue
import pandas as pd
from typing import Union
import sqlite3
from abc import abstractmethod
from contextlib import contextmanager


class SqlIOManagerBase(IOManager):
    def __init__(self, db_type: str, db_host: str):
        self.db_type = db_type
        self.db_host = db_host

    @staticmethod
    def _get_table_name(context: Union[InputContext, OutputContext]):
        # Eventually add schema here
        return context.asset_key.path[-1]

    def load_input(self, context: InputContext) -> (str, str, str):
        return {'db_type': self.db_type,
                'db_host': self.db_host,
                'context_name': self._get_table_name(context)}

    def handle_output(self, context: OutputContext, obj: (str, str)):
        with self._connection_context() as (conn, cursor):
            if 'drop_table' in obj and obj['drop_table'] == True:
                self._run_query(f'''drop table if exists {obj["table_name"]};''')
            self._run_query(obj['sql_query'])
            sample_query = f'''select * from {obj["table_name"]} limit 10;'''
            table_sample = pd.read_sql(sample_query, conn).to_markdown()

        metadata = {'db_type': MetadataValue.text(self.db_type),
                    'db_host': MetadataValue.text(self.db_host),
                    'table_name': MetadataValue.text(obj['table_name']),
                    'context_name': MetadataValue.text(self._get_table_name(context)),
                    'table_sample': MetadataValue.md(table_sample),
                    'sql_query': MetadataValue.md(f"```sql\n{obj['sql_query']}\n```"),
                    }
        context.add_output_metadata(metadata)

    @abstractmethod
    def _run_query(self, sql_query: str):
        raise NotImplementedError

    @abstractmethod
    @contextmanager
    def _connection_context(self):
        raise NotImplementedError


class SqliteIOManager(SqlIOManagerBase):
    def __init__(self, db_host: str):
        super().__init__(db_type='sqlite', db_host=db_host)

    @contextmanager
    def _connection_context(self):
        conn = sqlite3.connect(self.db_host)
        cursor = conn.cursor()
        try:
            yield conn, cursor
        finally:
            conn.commit()
            conn.close()

    def _run_query(self, query: str):
        with self._connection_context() as (conn, cursor):
            cursor.execute(query)
