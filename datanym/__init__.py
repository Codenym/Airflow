from dagster import Definitions, load_assets_from_modules

from .assets.IRS527 import irs_527
from .assets.HouseVotes import house_votes
from .resources.local_io_manager import LocalPickleIOManager
from .resources.local_to_s3_managers import LocalPickleToS3CSVIOManager
from .resources.s3_to_sql_manager import S3CSVtoSqliteIOManager, S3CSVtoRedshiftIOManager
from .resources.sql_running_managers import SqliteIOManager, RedshiftIOManager
from pathlib import Path
from .resources.duckpond import DuckPondIOManager, DuckDB

base_local_output_path = Path("output_data")
s3_bucket = 'datanym-pipeline'

defs = Definitions(
    assets=load_assets_from_modules([irs_527, house_votes]),
    resources={
        'local_io_manager': LocalPickleIOManager(local_directory_path=Path(base_local_output_path)),
        'DuckPondIOManager': DuckPondIOManager(bucket_name=s3_bucket, duckdb=DuckDB(), prefix='duckdb/'),
    },
)
