# System Libs
from pathlib import Path
import pandas as pd
from .core import (extract_file_from_zip,
                   download_file,
                   clean_extraction_directory)

from dagster import asset
@asset
def raw_527_data():
    """
    Downloads the IRS 527 data zip file, extracts it, and prepares the data for processing.
    """

    url = 'http://forms.irs.gov/app/pod/dataDownload/fullData'
    base_dir = Path("output_data/irs_527")

    zip_path = (base_dir / 'data.zip')
    extract_path = (base_dir / 'unzipped/')
    final_path = (base_dir / 'raw_FullDataFile.txt')

    download_file(url, zip_path)
    extract_file_from_zip(zip_path, extract_path)
    clean_extraction_directory(zip_path, extract_path, final_path)

    return str(final_path)


@asset
def data_dictionary():
    """
    Load mapping data needed for processing 527 data from an Excel file and build mappings for each record type.

    Returns:
    - dict: A dictionary containing mappings for each record type.
    """
    mappings_path = Path("input_data/IRS527/mappings.xlsx")
    record_types = ["1", "D", "R", "E", "2", "A", "B"]

    # Load all mappings from the Excel file into a dictionary
    mappings = {r: pd.read_excel(mappings_path, sheet_name=r) for r in record_types}

    # Build a mapping dictionary for each record type
    combined_mappings = {}
    for record_type, df in mappings.items():
        # Check if required columns are present
        required_columns = ['position', 'model_name', 'field_type']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Missing one of the required columns in record type {record_type}: {required_columns}")

        # Build the mapping for the current record type
        mapping = {row['position']: (row['model_name'], row['field_type']) for _, row in df.iterrows()}
        combined_mappings[record_type] = mapping

    return combined_mappings

# if __name__ == "__main__":
    raw_527_data()
    data_dictionary()