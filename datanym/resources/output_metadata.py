from dagster import (OutputContext, MetadataValue)
import pandas as pd
from typing import Union


def merge(dict1, dict2):
    return dict2.update(dict1)


def string_metadata(obj: str):
    assert isinstance(obj, str)
    return {"object": MetadataValue.text(obj)}


def dataframe_metadata(obj: pd.DataFrame):
    assert isinstance(obj, pd.DataFrame)
    return {
        "row_count": MetadataValue.int(obj.shape[0]),
        "column_count": MetadataValue.int(obj.shape[1]),
        "Top 10": MetadataValue.md(obj.head(10).to_markdown()),
    }


def list_metadata(obj: Union[list, tuple]):
    assert isinstance(obj, (list, tuple))
    return {
        "length": MetadataValue.int(len(obj)),
    }


def dict_metadata(obj: dict):
    assert isinstance(obj, dict)
    keys = pd.DataFrame({k: type(v) for k, v in obj.items()}, index=[0])
    return {"keys": MetadataValue.md(keys.to_markdown())}


def list_of_dicts_metadata(obj: Union[list, tuple]):
    try:
        assert isinstance(obj, (list, tuple))
        assert isinstance(obj[0], dict)
    except AssertionError as e:
        print("Error in list_of_dicts_metadata")
        print("obj is not a list or tuple of dicts")
        raise e

    keys = pd.DataFrame({k: type(v) for k, v in obj[0].items()}, index=[0])

    return {
        "Top 10": MetadataValue.md(pd.DataFrame(obj[:10]).to_markdown()),
        "list[0] keys": MetadataValue.md(keys.to_markdown())}


def list_of_lists_metadata(obj: Union[list, tuple]):
    try:
        assert isinstance(obj, (list, tuple))
        assert isinstance(obj[0], (list, tuple))
    except AssertionError as e:
        print("Error in list_of_dicts_metadata")
        print("obj is not a list or tuple of lists or tuples")
        raise e
    return {
        "Top 10": MetadataValue.md(pd.DataFrame(obj[:10]).to_markdown()),
        "list[0] length": MetadataValue.int(len(obj[0])),
    }


def add_metadata(context: OutputContext, obj, output_location: str):
    if isinstance(obj, str):
        context.add_output_metadata(string_metadata(obj))
    elif isinstance(obj, dict):
        context.add_output_metadata(dict_metadata(obj))
    elif isinstance(obj, pd.DataFrame):
        context.add_output_metadata(dataframe_metadata(obj))
    elif isinstance(obj, (list, tuple)):
        context.add_output_metadata(list_metadata(obj))
        if isinstance(obj[0], dict):
            context.add_output_metadata(list_of_dicts_metadata(obj))
        elif isinstance(obj[0], (list, tuple)):
            context.add_output_metadata(list_of_lists_metadata(obj))
    else:
        raise ValueError(
            f"Unsupported type: {type(obj)}.  Add obj type handing to resources/output_metadata.py in the add_metadata function")

    context.add_output_metadata(metadata={"output_location": MetadataValue.text(output_location)})
    context.add_output_metadata(metadata={"type": MetadataValue.text(str(type(obj)))})
