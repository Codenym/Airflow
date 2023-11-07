# System Libs
import csv
from collections import defaultdict
from datetime import datetime
import io
from pathlib import Path
import pandas as pd
from .core import (extract_file_from_zip,
                   download_file,
                   clean_extraction_directory)

from dagster import (asset,
                     get_dagster_logger,
                     AssetExecutionContext)


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
    """
    mappings_path = Path("datanym/assets/IRS527/input_data/mappings.xlsx")
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


def clean_cell(cell: str, cell_type: str) -> any:
    """
    Cleans the content of a cell and converts it to a specified type.

    :param cell: The cell content to be cleaned and converted.
    :param cell_type: The type to convert the cell content to. Options are:
                      'D' for datetime, 'I' for integer, 'N' for float, and
                      'S' (or any other value) for string.
    :returns: The cleaned and converted cell content. Datetime, integers, ints,
              and floats are cast to appropriate types.  Strings are
              uppercased, truncated to 50 characters, and 'null terms' are
              converted to None.
    """

    null_terms = ['N/A', 'NOT APPLICABLE', 'NA', 'NONE', 'NOT APPLICABE', 'NOT APLICABLE', 'N A', 'N-A']

    if cell_type == 'D':
        try:
            return datetime.strptime(cell, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return datetime.strptime(cell, '%Y%m%d')
    elif cell_type == 'I':
        return int(cell)
    elif cell_type == 'N':
        return float(cell)
    else:
        cell = cell.upper()
        if len(cell) > 50:
            cell = cell[0:50]
        if cell in null_terms:
            cell = None
    return cell


def parse_row(row: list, mapping: list) -> dict:
    """
    Parses a single row into a dictionary according to the provided mapping.

    Each element in the row is cleaned and transformed based on a corresponding function
    found in the mapping, which associates column indices with a tuple consisting of the
    target dictionary key and a transformation function.

    :param row: List or tuple representing the row to be parsed.
    :param mapping: List of tuples where each tuple contains a string (as dictionary key)
                    and a function (for data transformation). The index of each tuple in
                    the list corresponds to the column index in the row.
    :returns: A dictionary of cleaned column values with keys corresponding to the mapping.
    """
    logger = get_dagster_logger()

    parsed_row = {}
    for i, cell in enumerate(row):
        try:
            parsed_cell = clean_cell(cell, mapping[i][1])
            parsed_row[mapping[i][0]] = parsed_cell
        except KeyError as e:
            if cell == '':
                pass
            else:
                logger.error(f"Error parsing cell: {cell} at position {i} in row: {row}")
                raise e
        except Exception as e:
            logger.error(f"Error parsing cell: {cell} at position {i} in row: {row}")
            raise e
    return parsed_row


def process_row(row: list, mappings: dict, records: dict) -> None:
    """
    Processes a single row based on its form type and updates the records collection.

    :param row: A list or tuple representing a single data row, where the first element is the form type.
    :param mappings: A dictionary mapping form types to their corresponding parsers.
    :param records: A dictionary of lists, where each key is a form type and each value is a list of records.
    """
    logger = get_dagster_logger()

    form_type = str(row[0])

    if form_type in ('H', 'F'):
        logger.info(row)  # metadata
    else:
        parsed_row = parse_row(row, mappings[form_type])
        records[form_type].append(parsed_row)


def fix_malformed(line: str) -> str:
    """
    Corrects specific instances of malformed csv strings in a given line.

    :param line: The input string that may contain malformed substrings.
    :return: The corrected line with all specified malformed substrings replaced.
    """
    malformed = (
        ('|"I Factor|', '|"I Factor"|'),
        ('''|"N/A'|''', '''|"N/A"|'''),
        ('''|"522 Highland Avenue |''', '''|"522 Highland Avenue"|'''),
        ('''|"AV-TECH INDUSTRIES|''', '''|"AV-TECH INDUSTRIES"|'''),
        ('''|"c/o Moxie Innovative|''', '''|"c/o Moxie Innovative"|''')
    )

    for m in malformed:
        if line.count(m[0]) > 0:
            line = line.replace(m[0], m[1])

    return line


@asset
def clean_527_data(context: AssetExecutionContext, raw_527_data: str, data_dictionary: dict):
    """
    Processes the raw_527_data file using the provided data_dictionary mappings for each form type.
    """
    logger = get_dagster_logger()
    records = defaultdict(list)
    with io.open(raw_527_data, 'r', encoding='ISO-8859-1') as raw_file:
        reader = csv.reader(map(fix_malformed, raw_file), delimiter='|')
        try:
            for i, row in enumerate(reader):
                if len(row) == 0:
                    continue
                if row[0] in data_dictionary.keys():
                    process_row(previous_row, data_dictionary, records)
                    previous_row = row
                elif row[0] in ('H', 'F'):
                    previous_row = row
                    if row[0] == 'H':
                        context.add_output_metadata(
                            metadata={
                                "Header Transmission Date": row[1],
                                "Header Transmission Time": row[2],
                                "File ID Modifier": row[3],
                            }
                        )
                    elif row[0] == 'F':
                        context.add_output_metadata(
                            metadata={
                                "Footer Transmission Date": row[1],
                                "Footer Transmission Time": row[2],
                                "Footer Record Count": row[3],
                            }
                        )
                else:
                    previous_row = previous_row[:-1] + [previous_row[-1] + row[0]] + row[1:]
                if i % 1000000 == 0:
                    logger.info(f"Processed {i / 1000000}M rows processed so far.")
        except Exception as e:
            logger.error(f"Error processing {i}th row: {row}")
            raise e

    return records
