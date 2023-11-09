from pathlib import Path
from typing import Union


def get_file_name(context) -> str:
    """
    Constructs the file name for a given output or input context.

    :param context: The context containing metadata about the pipeline step.
    :return: The file name as a string.
    """
    if len(context.asset_key.path) == 1:
        return context.asset_key.path[0]
    else:
        print(context.asset_key.path)
        raise ValueError("Asset key path must be of length 1")


def get_file_path(context, directory_path: Union[str, Path]) -> Path:
    """
    Constructs the full file path for a given output or input context.

    :param context: The context containing metadata about the pipeline step.
    :param directory_path: The path to the directory
    :return: The file path as a pathlib.Path object.
    """
    return Path(directory_path) / get_file_name(context)
