from dagster import IOManager, OutputContext, InputContext
import pickle
from pathlib import Path
from .utils import get_file_path

class LocalPickleIOManager(IOManager):
    """
    An IOManager that handles the serialization and deserialization of
    pipeline data using the pickle module. It stores and retrieves data to and
    from a specified local file system path.

    :param local_directory_path: The base directory path for storing output data files.
    """
    def __init__(self, local_directory_path: str):
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
        output_path = get_file_path(context, self.local_directory_path)
        with open(output_path, "wb") as handle:
            pickle.dump(obj, handle, 4)

        context.add_output_metadata(
            metadata={
                "output_location": output_path
            }
        )

    def load_input(self, context: InputContext):
        """
         Reads and deserializes data from a file to serve as input for a pipeline step.

         :param context: The context of the pipeline step consuming the input.
         :return: The deserialized object.
         """
        with open(get_file_path(context, self.local_directory_path), "rb") as handle:
            return pickle.load(handle)
