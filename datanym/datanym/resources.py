from dagster import FilesystemIOManager


local_io_manager = FilesystemIOManager(
    base_dir="output_data",

)
