# from dagster import (
#     AssetSelection,
#     Definitions,
#     ScheduleDefinition,
#     define_asset_job,
#     load_assets_from_modules,
# )
# from pathlib import Path

# git_manager = GithubToLocalIOManager(
#     local_directory_path=Path("data/congress-legislators"), 
#     git_url="https://github.com/unitedstates/congress-legislators")

# defs = Definitions(
#     assets=all_assets,
#     resources={
#         'github_to_local_io_manager': git_manager
#     }
# )
from .house_votes import  *