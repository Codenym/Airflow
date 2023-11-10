import pandas as pd
from dagster import IOManager, OutputContext, InputContext
import pickle
from .output_metadata import (
    add_path_metadata,
    add_dict_metadata,
    add_dataframe_metadata,
)
from pathlib import Path


class LocalPickleIOManager(IOManager):
    """
    An IOManager that handles the serialization and deserialization of
    pipeline data using the pickle module. It stores and retrieves data to and
    from a specified local file system path.

    :param local_directory_path: The base directory path for storing output data files.
    """

    def __init__(self, local_directory_path: Path):
        """
        :param local_directory_path: The file system path where data files will be stored.
        """
        self.local_directory_path = local_directory_path

    def handle_output(self, context: OutputContext, obj):
        """
        Serializes and writes the output of a pipeline step to a file.

        :param context: The context of the pipeline step producing the output.
        :param obj: The object to be serialized and stored.
        """

        output_path = self.local_directory_path / context.asset_key.path[-1]
        with open(output_path, "wb") as handle:
            pickle.dump(obj, handle, 4)

        context.add_output_metadata(metadata={"out|io manager path": output_path})
        if isinstance(obj, Path):           add_path_metadata(context=context, obj=obj)
        elif isinstance(obj, dict):         add_dict_metadata(context=context, obj=obj)
        elif isinstance(obj, pd.DataFrame): add_dataframe_metadata(context=context, obj=obj)
        else: raise ValueError(f"Unsupported type: {type(obj)}.  Add type LocalPickleIOManager/handle_output")

    def load_input(self, context: InputContext):
        """
         Reads and deserializes data from a file to serve as input for a pipeline step.

         :param context: The context of the pipeline step consuming the input.
         :return: The deserialized object.
         """
        with open(self.local_directory_path / context.asset_key.path[-1], "rb") as handle:
            return pickle.load(handle)
