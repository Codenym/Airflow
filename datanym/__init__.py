from dagster import Definitions, load_assets_from_modules

from .assets.IRS527 import irs_527
from .assets.HouseVotes import house_votes
from .resources.local_io_manager import local_io_manager

defs = Definitions(
    assets=load_assets_from_modules([irs_527,house_votes]),
    resources={
        'local_io_manager': local_io_manager,
    },
)
