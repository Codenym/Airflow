from dagster import IOManager, OutputContext, InputContext, MetadataValue
import pandas as pd
from typing import Union
import sqlite3
from .utils import get_file_name
from abc import abstractmethod


class SqlIOManagerBase(IOManager):
    def __init__(self, db_type: str, db_host: str):
        self.db_type = db_type
        self.db_host = db_host

    @staticmethod
    def _get_table_name(context: Union[InputContext, OutputContext]):
        # Eventually add schema here
        return get_file_name(context)

    def load_input(self, context: InputContext) -> (str, str, str):
        return {'db_type': self.db_type,
                'db_host': self.db_host,
                'context_name': self._get_table_name(context)}

    def handle_output(self, context: OutputContext, obj: (str, str)):
        if 'drop_table' in obj and obj['drop_table']==True:
            self._run_query(f'''drop table if exists {obj["table_name"]};''')
        self._run_query(obj['sql_query'])
        sample_query = f'''select * from {obj["table_name"]} limit 10;'''
        table_sample = pd.read_sql(sample_query,self._connect()).to_markdown()

        metadata = {'db_type': MetadataValue.text(self.db_type),
                    'db_host': MetadataValue.text(self.db_host),
                    'context_name': MetadataValue.text(self._get_table_name(context)),
                    'sql_query': MetadataValue.md(f"```sql\n{obj['sql_query']}\n```"),
                    'table_name': MetadataValue.text(obj['table_name']),
                    'table_sample': MetadataValue.md(table_sample)
                    }
        context.add_output_metadata(metadata)

    @abstractmethod
    def _connect(self):
        raise NotImplementedError

    @abstractmethod
    def _run_query(self, sql_query: str):
        raise NotImplementedError


class SqliteIOManager(SqlIOManagerBase):
    def __init__(self, db_host: str):
        super().__init__(db_type='sqlite', db_host=db_host)

    def _connect(self):
        return sqlite3.connect(self.db_host)

    def _run_query(self, query: str):
        conn = self._connect()
        c = conn.cursor()
        c.execute(query)
        conn.commit()
        conn.close()
