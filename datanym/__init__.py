from dagster import Definitions, load_assets_from_modules

from .assets.IRS527 import irs_527
<<<<<<< HEAD
from .assets.HouseVotes import house_votes
from .resources.local_io_manager import local_io_manager
=======
from .resources.local_io_manager import LocalPickleIOManager
from .resources.local_to_s3_managers import LocalPickleToS3CSVIOManager
from .resources.s3_to_sqlite_manager import S3CSVtoSqliteIOManager
from .resources.sql_running_managers import SqliteIOManager
from pathlib import Path

base_local_output_path = Path("output_data")
s3_bucket = 'datanym-pipeline'
>>>>>>> main

defs = Definitions(
    assets=load_assets_from_modules([irs_527,house_votes]),
    resources={
        'local_io_manager': LocalPickleIOManager(local_directory_path=Path(base_local_output_path) / 'irs_527'),
        'local_to_s3_io_manager': LocalPickleToS3CSVIOManager(
            local_directory_path=Path(base_local_output_path) / 'irs_527',
            s3_bucket=s3_bucket,
            s3_directory='irs_527/'),
        's3_to_sqlite_manager': S3CSVtoSqliteIOManager('sqlite_527.db'),
        'sqlite_manager': SqliteIOManager('sqlite_527.db')
    },
)
