from dagster import IOManager, OutputContext, InputContext
from huggingface_hub import upload_file
import os
from pathlib import Path

os.environ['HF_TOKEN'] = "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"
from dbcreator.duck_db import FlexPath, create_duckdb_database_from_s3_parquet


class LocalToHFManager(IOManager):
    def handle_output(self, context: OutputContext, from_to_info: tuple[Path, Path]):
        # from_to_info is a tuple of (from, to).
        # For example, (Path("some/local/path/setup.py"), "Codenym/reponame/setup.py")
        from_path, to_path = from_to_info
        upload_file(
            path_or_fileobj=from_path,
            path_in_repo=str(Path(*to_path.parts[2:])),
            repo_id=str(Path(*to_path.parts[:2])),
            repo_type='dataset'
        )

    def load_input(self, context: InputContext):
        raise Exception("load_input not implemented")


