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
                'table_name': self._get_table_name(context)}

    def handle_output(self, context: OutputContext, obj):
        metadata = {}
        with self._connection_context() as (conn, cursor):
            if isinstance(obj['sql_queries'], str):
                obj['sql_queries'] = [obj['sql_queries']]
            for sql_query in obj['sql_queries']:
                self._run_query(sql_query)

            if 'table_name' in obj:
                table_sample = pd.read_sql( f'''select * from {obj["table_name"]} limit 10;''', conn).to_markdown()
                metadata.update({'table_name': MetadataValue.text(obj['table_name']),
                                 'table_sample': MetadataValue.md(table_sample)})
        if 'sql_file' in obj:
            metadata.update({'sql_file': MetadataValue.path(obj['sql_file'])})

        sql_text = '\n\n'.join(obj['sql_queries'])
        metadata.update({'db_type': MetadataValue.text(self.db_type),
                         'db_host': MetadataValue.text(self.db_host),
                         'context_name': MetadataValue.text(self._get_table_name(context)),
                         'sql_queries': MetadataValue.md(f"```sql\n{sql_text}\n```"),
                         })
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
        cursor.execute('pragma cache_size = 40000;')
        cursor.execute('pragma journal_mode=wal;')
        cursor.execute('pragma synchronous = normal;')
        cursor.execute('pragma temp_store = memory;')
        cursor.execute('pragma mmap_size = 30000000000')
        conn.commit()
        try:
            yield conn, cursor
        finally:
            conn.commit()
            cursor.execute('pragma optimize;')
            cursor.execute('pragma vacuum;')
            conn.commit()
            conn.close()

    def _run_query(self, query: str):
        with self._connection_context() as (conn, cursor):
            cursor.execute(query)
