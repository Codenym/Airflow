from dagster import IOManager, OutputContext, InputContext, MetadataValue
import pandas as pd
from typing import Union
import sqlite3
from abc import abstractmethod
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import credstash
from sqlalchemy import text

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
        metadata = {}
        with self._connection_context() as conn:
            for sql_query in obj['sql_query'].split(';'):
                if sql_query.strip() == '': continue
                self._run_query(sql_query)
                conn.commit()

            if 'table_name' in obj:
                table_sample = pd.read_sql( f'''select * from {obj["table_name"]} limit 10;''', conn).to_markdown()
                metadata.update({'table_name': MetadataValue.text(obj['table_name']),
                                 'table_sample': MetadataValue.md(table_sample)})
        if 'sql_file' in obj:
            metadata.update({'sql_file': MetadataValue.path(obj['sql_file'])})

        metadata.update({'db_type': MetadataValue.text(self.db_type),
                         'db_host': MetadataValue.text(self.db_host),
                         'context_name': MetadataValue.text(self._get_table_name(context)),
                         'sql_query': MetadataValue.md(f"```sql\n{obj['sql_query']}\n```"),
                         })
        context.add_output_metadata(metadata)

    @abstractmethod
    def _run_query(self, sql_query: str):
        raise NotImplementedError

    @abstractmethod
    @contextmanager
    def _connection_context(self):
        raise NotImplementedError



class RedshiftIOManager(SqlIOManagerBase):
    def __init__(self, db_host: str):
        super().__init__(db_type='redshift', db_host=db_host)

        redshift_password = credstash.getSecret('database.aws_redshiftserverless.password', region='us-east-1', profile_name='codenym')
        redshift_username = credstash.getSecret('database.aws_redshiftserverless.username', region='us-east-1', profile_name='codenym')
        redshift_port = credstash.getSecret('database.aws_redshiftserverless.port', region='us-east-1', profile_name='codenym')

        self.url_object = URL.create(drivername='redshift+redshift_connector',
                                username=redshift_username,
                                password=redshift_password,
                                host=self.db_host,
                                database='dev',
                                port=redshift_port)


    @contextmanager
    def _connection_context(self):
        engine = create_engine(self.url_object, future=True)
        try:
            with engine.connect() as conn:
                yield conn
        finally:
            pass

    def _run_query(self, query: str):
        with self._connection_context() as conn:
            conn.execute(text(query))
            conn.commit()

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
