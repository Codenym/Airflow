from dagster import Definitions, load_assets_from_modules
from .assets import IRS527
from .assets.HouseVotes import house_votes
from .resources.local_io_manager import LocalPickleIOManager
from pathlib import Path
from .resources.duckpond import DuckPondIOManager, DuckDB

base_local_output_path = Path("output_data")
s3_bucket = 'datanym-pipeline'

defs = Definitions(
    assets=load_assets_from_modules([IRS527, house_votes]),
    resources={
        'local_io_manager': LocalPickleIOManager(local_directory_path=Path(base_local_output_path)),
        'DuckPondIOManager': DuckPondIOManager(bucket_name=s3_bucket, duckdb=DuckDB(), prefix='duckdb/'),
    },
)
