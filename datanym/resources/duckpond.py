import os.path

from duckdb import connect
from dagster import IOManager, MetadataValue
import pandas as pd
from sqlescapy import sqlescape
from string import Template
from typing import Mapping
from .output_metadata import add_metadata
from pathlib import Path


class FlexPath(type(Path())):

    def is_s3(self):
        return self.parts[0] == 's3:'

    def __str__(self):
        if self.is_s3():
            return f"s3://{super().__str__()[4:]}"
        else:
            return super().__str__()

    def get_s3_bucket(self):
        if self.is_s3():
            return self.parts[1]
        else:
            raise Exception("Not an S3 path")

    def get_s3_prefix(self):
        if self.is_s3():
            return '/'.join(self.parts[2:])
        else:
            raise Exception("Not an S3 path")


class SQL:
    def __init__(self, sql, **bindings):
        for binding in bindings:
            assert binding in sql
        self.sql = sql
        self.bindings = bindings
        # assert partitions is None or isinstance(partitions, list) or isinstance(partitions, tuple)
        # self.partitions = partitions


def sql_to_string(s: SQL) -> str:
    replacements = {}
    for key, value in s.bindings.items():
        if isinstance(value, pd.DataFrame):
            replacements[key] = f"df_{id(value)}"
        elif isinstance(value, SQL):
            replacements[key] = f"({sql_to_string(value)})"
        elif isinstance(value, str):
            replacements[key] = f"'{sqlescape(value)}'"
        elif isinstance(value, (int, float, bool)):
            replacements[key] = str(value)
        elif value is None:
            replacements[key] = "null"
        else:
            raise ValueError(f"Invalid type for {key}")
    return Template(s.sql).safe_substitute(replacements)


def collect_dataframes(s: SQL) -> Mapping[str, pd.DataFrame]:
    dataframes = {}
    for key, value in s.bindings.items():
        if isinstance(value, pd.DataFrame):
            dataframes[f"df_{id(value)}"] = value
        elif isinstance(value, SQL):
            dataframes.update(collect_dataframes(value))
    return dataframes


class DuckDB:
    def __init__(self, options="", db_location=":memory:"):
        self.options = options
        self.db_location = db_location

    def query(self, select_statement: SQL):
        db = connect(self.db_location)
        db.query("install httpfs; load httpfs;")
        db.query("install aws; load aws;")
        db.query("CALL load_aws_credentials('codenym');")

        db.query(self.options)

        dataframes = collect_dataframes(select_statement)
        for key, value in dataframes.items():
            db.register(key, value)

        result = db.query(sql_to_string(select_statement))
        if result is None:
            return
        return result.df()


class DuckPondIOManager(IOManager):
    def __init__(self, bucket_name: str, duckdb: DuckDB, prefix=""):
        self.bucket_name = bucket_name
        self.duckdb = duckdb
        self.prefix = prefix

    def _get_s3_url(self, context):
        if context.has_asset_key:
            id = context.get_asset_identifier()
        else:
            id = context.get_identifier()
        return f"s3://{self.bucket_name}/{self.prefix}{'/'.join(id)}.parquet"

    def handle_output(self, context, select_statement: SQL):
        if select_statement is None:
            return

        if not isinstance(select_statement, SQL):
            raise ValueError(
                f"Expected asset to return a SQL; got {select_statement!r}"
            )

        self.duckdb.query(
            SQL(
                sql="copy $select_statement to $url (format parquet)",
                select_statement=select_statement,
                url=self._get_s3_url(context)
            )
        )
        sample = self.duckdb.query(
            SQL("select * from read_parquet($url) limit 10", url=self._get_s3_url(context))).to_markdown()
        metadata = {'select_statement': MetadataValue.md(f'```sql\n{sql_to_string(select_statement)}\n```'),
                    'url': MetadataValue.text(self._get_s3_url(context)),
                    'sample': MetadataValue.md(sample)
                    }
        add_metadata(context, metadata)

    def load_input(self, context) -> SQL:
        return SQL("select * from read_parquet($url)", url=self._get_s3_url(context))


def get_table_name_from_path(fpath: FlexPath):
    '''
    Args:
        fpath: Path to the file (e.g. /home/user/data/blah_co.parquet, or s3://duckdb-data/blah_co.parquet)
            - file name must be in the format schemaname_tablename.extension

    Returns:
        schema_name: The schema name (e.g. blah_co)
        table_name: The table name (e.g. 2020-01-01)

    '''
    schema_name = fpath.stem.split('_')[0]
    table_name = fpath.stem[len(schema_name) + 1:]
    return schema_name, table_name


class DuckDBCreatorIOManager(IOManager):
    def __init(self, db_fpath, aws_profile='codenym'):
        self.aws_profile = aws_profile

    def get_name(self, context):
        if context.has_asset_key:
            cid = context.get_asset_identifier()
        else:
            cid = context.get_identifier()
        return f"{'/'.join(cid)}.duckdb"

    def handle_output(self, context, select_statements: list[SQL]):
        # from_to_info is a tuple of (from, to).
        # For example, (FlexPath("s3://datanym/duckdb/"), "somedb.duckdb")

        if os.path.exists(self.get_name(context)):
            os.remove(self.get_name(context))

        db = DuckDB(db_location=self.get_name(context))
        schemas = db.query(SQL('select distinct schema_name from information_schema.schemata'))
        for select_statement in select_statements:
            schema_name, table_name = get_table_name_from_path(FlexPath(select_statement.bindings['url']))

            if schema_name not in list(schemas['schema_name']):
                print((schema_name,type(schemas['schema_name']),list(schemas['schema_name'])))
                db.query(SQL(f"create schema {schema_name}"))
                schemas = db.query(SQL('select distinct schema_name from information_schema.schemata'))

            qry = SQL(f"create or replace table {schema_name}.{table_name} as $select_statement;",
                      select_statement=select_statement)
            db.query(qry)

    def load_input(self, context):
        return self.get_name(context)
