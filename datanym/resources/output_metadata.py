from dagster import (OutputContext, InputContext, MetadataValue)
import pandas as pd
from typing import Union
from pathlib import Path


def add_metadata(context, metadata, add_context_to_keys=False) -> None:
    if isinstance(context, OutputContext):
        if add_context_to_keys: metadata = {f"out|{k}": v for k, v in metadata.items()}
        context.add_output_metadata(metadata=metadata)
    elif isinstance(context, InputContext):
        if add_context_to_keys: metadata = {f"inp|{k}": v for k, v in metadata.items()}
        context.add_input_metadata(metadata=metadata)
    else:
        raise ValueError(
            f"Unsupported type: {type(context)}.  Add context type handing to add_context_to_keys function")


def add_string_metadata(context: Union[OutputContext, InputContext], obj: str) -> None:
    assert isinstance(obj, str)
    metadata = {"object": MetadataValue.text(obj)}
    add_metadata(context=context, metadata=metadata)


def add_path_metadata(context: Union[OutputContext, InputContext], obj: Path) -> None:
    assert isinstance(obj, Path)
    metadata = {"path": MetadataValue.path(obj)}
    add_metadata(context=context, metadata=metadata)


def add_dataframe_metadata(context: Union[OutputContext, InputContext], obj: pd.DataFrame) -> None:
    assert isinstance(obj, pd.DataFrame)
    metadata = {
        "df shape": MetadataValue.text(f"{obj.shape[0]} rows x {obj.shape[1]} columns"),
        "df head": MetadataValue.md(obj.head(10).to_markdown()),
    }
    add_metadata(context=context, metadata=metadata)


def add_list_metadata(context: Union[OutputContext, InputContext], obj: list) -> None:
    assert isinstance(obj, list)
    metadata = {"length": MetadataValue.int(len(obj))}
    add_metadata(context=context, metadata=metadata)


def add_tuple_metadata(context: Union[OutputContext, InputContext], obj: tuple) -> None:
    assert isinstance(obj, tuple)
    metadata = {"length": MetadataValue.int(len(obj))}
    add_metadata(context=context, metadata=metadata)


def add_dict_metadata(context: Union[OutputContext, InputContext], obj: dict) -> None:
    assert isinstance(obj, dict)
    keys = pd.DataFrame({k: type(v) for k, v in obj.items()}, index=['type']).transpose()
    metadata = {"keys": MetadataValue.md(keys.to_markdown())}
    add_metadata(context=context, metadata=metadata)
