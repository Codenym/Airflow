from dagster import Definitions, load_assets_from_modules
from .assets import IRS527
from .assets import HouseVotes
from .resources.local_io_manager import LocalPickleIOManager
from pathlib import Path
from .resources.duckpond import DuckPondIOManager, DuckDB, DuckDBCreatorIOManager
from .resources.publish import LocalToHFManager

base_local_output_path = Path("output_data")
s3_bucket = "datanym-pipeline"

duckdb_options = """
SET temp_directory = 'tmp_offload_duckdb.tmp'; 
SET preserve_insertion_order = false; 
SET enable_progress_bar = true;
"""

defs = Definitions(
    assets=load_assets_from_modules([IRS527, HouseVotes]),
    resources={
        "local_io_manager": LocalPickleIOManager(
            local_directory_path=Path(base_local_output_path)
        ),
        "DuckPondIOManager": DuckPondIOManager(
            bucket_name=s3_bucket,
            duckdb=DuckDB(options=duckdb_options),
            prefix="duckdb/",
        ),
        "duckDB_creator_io_manager": DuckDBCreatorIOManager(),
        "local_to_hf_io_manager": LocalToHFManager(),
    },
)
