from dagster import Definitions, load_assets_from_modules

from .assets.IRS527 import irs_527
from .resources.local_io_manager import LocalPickleIOManager
from .resources.local_to_s3_managers import LocalPickleToS3CSVIOManager

local_directory_path = "output_data"
s3_bucket = 'datanym-pipeline'

local_io_manager = LocalPickleIOManager(local_directory_path=local_directory_path)

defs = Definitions(
    assets=load_assets_from_modules([irs_527]),
    resources={
        'local_io_manager': local_io_manager,
        'local_to_s3_io_manager': LocalPickleToS3CSVIOManager(local_directory_path=local_directory_path,
                                                              s3_bucket=s3_bucket,
                                                              s3_directory='irs_527/')
    },
)
