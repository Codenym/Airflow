from dagster import IOManager, OutputContext, InputContext
import pickle
from pathlib import Path


class LocalPickleIOManager(IOManager):
    """
    An IOManager that handles the serialization and deserialization of
    pipeline data using the pickle module. It stores and retrieves data to and
    from a specified local file system path.

    :param path: The base directory path for storing output data files.
    """
    def __init__(self, path: str):
        """
        :param path: The file system path where data files will be stored.
        """
        self.path = path

    def _get_path(self, context) -> Path:
        """
        Constructs the full file path for a given output or input context.

        :param context: The context containing metadata about the pipeline step.
        :return: The file path as a pathlib.Path object.
        """
        if len(context.asset_key.path) == 1:
            return Path(self.path) / context.asset_key.path[0]
        else:
            print(context.asset_key.path)
            raise ValueError("Asset key path must be of length 1")

    def handle_output(self, context: OutputContext, obj):
        """
        Serializes and writes the output of a pipeline step to a file.

        :param context: The context of the pipeline step producing the output.
        :param obj: The object to be serialized and stored.
        """
        with open(self._get_path(context), "wb") as handle:
            pickle.dump(obj, handle, 4)

    def load_input(self, context: InputContext):
        """
         Reads and deserializes data from a file to serve as input for a pipeline step.

         :param context: The context of the pipeline step consuming the input.
         :return: The deserialized object.
         """
        with open(self._get_path(context), "rb") as handle:
            return pickle.load(handle)


local_io_manager = LocalPickleIOManager(path="output_data")
