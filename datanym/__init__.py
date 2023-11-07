from dagster import Definitions, load_assets_from_modules


from .assets.IRS527 import irs_527
from .assets.resources import local_io_manager

defs = Definitions(
    assets=load_assets_from_modules([irs_527]),
    resources={
        'local_io_manager': local_io_manager,
    },
)
