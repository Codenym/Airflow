import logging
import os.path
from dagster import IOManager, MetadataValue, get_dagster_logger
from .output_metadata import add_metadata
from ezduckdb import DuckDB, S3AwarePath, SQL


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

    def handle_output(self, context, obj: SQL):
        logger = get_dagster_logger()
        if obj is None:
            return

        if not isinstance(obj, SQL):
            raise ValueError(f"Expected asset to return a SQL; got {obj}")

        logger.info(f"Writing to {self._get_s3_url(context)}")

        qry = SQL(
                sql="copy $select_statement to $url (format parquet)",
                select_statement=obj,
                url=self._get_s3_url(context),
            )
        logger.info(f"duckdb.aws_profile: {self.duckdb.aws_profile}")
        logger.info(f"duckdb.aws_env_vars: {self.duckdb.aws_env_vars}")
        logger.info(f"DAGSTER_CLOUD_DEPLOYMENT_NAME: {os.getenv('DAGSTER_CLOUD_DEPLOYMENT_NAME')}")
        logger.info(f"Running query: {qry.to_string()}")
        self.duckdb.query(qry)

        sample = self.duckdb.query(
            SQL(
                "select * from read_parquet($url) limit 10",
                url=self._get_s3_url(context),
            )
        )
        metadata = {
            "select_statement": MetadataValue.md(f"```sql\n{obj.to_string()}\n```"),
            "url": MetadataValue.text(self._get_s3_url(context)),
            "sample": MetadataValue.md(sample.to_markdown()),
        }
        add_metadata(context, metadata)

    def load_input(self, context) -> SQL:
        return SQL("select * from read_parquet($url)", url=self._get_s3_url(context))


def get_table_name_from_path(fpath: S3AwarePath):
    """
    Args:
        fpath: Path to the file (e.g. /home/user/data/blah_co.parquet, or s3://duckdb-data/blah_co.parquet)
            - file name must be in the format schemaname_tablename.extension

    Returns:
        schema_name: The schema name (e.g. blah_co)
        table_name: The table name (e.g. 2020-01-01)

    """

    schema_name = fpath.stem.split("_")[0]
    table_name = fpath.stem[len(schema_name) + 1 :]
    return schema_name, table_name


class DuckDBCreatorIOManager(IOManager):
    def __init__(self, aws_profile="codenym"):
        self.aws_profile = aws_profile

    def get_name(self, context):
        if context.has_asset_key:
            cid = context.get_asset_identifier()
        else:
            cid = context.get_identifier()
        return f"{'/'.join(cid)}.duckdb"

    def handle_output(self, context, obj: list[SQL]):
        # from_to_info is a tuple of (from, to).
        # For example, (S3AwarePath("s3://datanym/duckdb/"), "somedb.duckdb")
        logger = get_dagster_logger()

        if os.path.exists(self.get_name(context)):
            os.remove(self.get_name(context))
        get_schemas_qry = "select distinct schema_name from information_schema.schemata"
        if os.getenv('DAGSTER_CLOUD_DEPLOYMENT_NAME') == 'prod':
            db = DuckDB(aws_env_vars=True, db_location=self.get_name(context))
        else:
            db = DuckDB(aws_profile='codenym', db_location=self.get_name(context))

        schemas = db.query(SQL(get_schemas_qry))
        for select_statement in obj:
            schema_name, table_name = S3AwarePath(
                select_statement.bindings["url"]
            ).get_table_name()
            logger.info(f"schema_name: {schema_name}, table_name: {table_name}")

            if schema_name not in list(schemas["schema_name"]):
                db.query(SQL(f"create schema {schema_name}"))
                logger.info(f"Created schema {schema_name}")
                schemas = db.query(SQL(get_schemas_qry))

            qry = SQL(
                f"create or replace table {schema_name}.{table_name} as $select_statement;",
                select_statement=select_statement,
            )
            logger.info(f"Running query: {qry.to_string()}")
            db.query(qry)

    def load_input(self, context):
        return self.get_name(context)
