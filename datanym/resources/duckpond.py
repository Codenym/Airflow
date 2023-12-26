from duckdb import connect
from dagster import IOManager, MetadataValue
import pandas as pd
from sqlescapy import sqlescape
from string import Template
from typing import Mapping
import credstash
from .output_metadata import add_metadata


class SQL:
    def __init__(self, sql, **bindings):
        for binding in bindings:
            assert binding in sql
        self.sql = sql
        self.bindings = bindings


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
    def __init__(self, options=""):
        self.options = options

    def query(self, select_statement: SQL):
        db = connect(":memory:")
        db.query("install httpfs; load httpfs;")
        db.query("install aws; load aws;")
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

        # table_sample = pd.read_sql(f'''select * from {obj["table_name"]} limit 10;''', conn).to_markdown()

        metadata = {'select_statement': MetadataValue.md(f'```sql\n{sql_to_string(select_statement)}\n```'),
                    'url': MetadataValue.text(self._get_s3_url(context))
                    }

        add_metadata(context, metadata)


    def load_input(self, context) -> SQL:
        return SQL("select * from read_parquet($url)", url=self._get_s3_url(context))
