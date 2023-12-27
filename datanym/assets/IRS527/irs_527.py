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
from ...resources.duckpond import SQL

from dagster import (asset,
                     AssetOut,
                     multi_asset,
                     get_dagster_logger,
                     )


@asset(group_name="IRS_527", io_manager_key="local_io_manager")
def raw_527_data() -> Path:
    """
    Downloads the IRS 527 data zip file, extracts it, and prepares the data for processing.
    """

    url = 'https://forms.irs.gov/app/pod/dataDownload/fullData'
    base_dir = Path("output_data/irs_527")

    zip_path = (base_dir / 'data.zip')
    extract_path = (base_dir / 'unzipped/')
    final_path = (base_dir / 'raw_FullDataFile.txt')

    download_file(url, zip_path)
    extract_file_from_zip(zip_path, extract_path)
    clean_extraction_directory(zip_path, extract_path, final_path)

    return final_path


@asset(group_name="IRS_527", io_manager_key="local_io_manager")
def data_dictionary() -> dict:
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


def process_row(row: list, mappings: dict, records: dict) -> None:
    if row[0] == 'H': return
    null_terms = ['N/A', 'NOT APPLICABLE', 'NA', 'NONE', 'NOT APPLICABE', 'NOT APLICABLE', 'N A', 'N-A', ]
    mapping = mappings[row[0]]
    parsed_row = {}
    for i, cell in enumerate(row):
        cell = None if cell in null_terms else cell
        try:
            parsed_row[mapping[i][0]] = cell
        except KeyError as e:
            if cell == '': pass
            else:
                print(cell)
                raise e
    records[row[0]].append(parsed_row)


def fix_malformed_row(line: str) -> str:
    malformed = (
        ('|"I Factor|', '|"I Factor"|'),
        ('''|"N/A'|''', '''|"N/A"|'''),
        ('''|"522 Highland Avenue |''', '''|"522 Highland Avenue"|'''),
        ('''|"AV-TECH INDUSTRIES|''', '''|"AV-TECH INDUSTRIES"|'''),
        ('''|"c/o Moxie Innovative|''', '''|"c/o Moxie Innovative"|'''),
        ('''|"423 Georgia Ave SE |''', '''|"423 Georgia Ave SE"|'''),
        ('''|"Chris Andrews|''', '''|"Chris Andrews"|'''),
        ('''|"Petroleum|''', '''|"Petroleum"|'''),
        ('''|"J. Conly and Associates|''', '''|"J. Conly and Associates"|'''),
        ('''|"Ubs Securities|''', '''|"Ubs Securities"|'''),
        ('''|"P.O. Box 619616 |''', '''|"P.O. Box 619616"|'''),
        ('''|"WF Enterprises|''', '''|"WF Enterprises"|'''),
        ('''|"225 Broadway Ste 1410 New York, NY|''', '''|"225 Broadway Ste 1410 New York, NY"|'''),
        ('''|"701 W Jackson Blvd Apt 407G Chicag|''', '''|"701 W Jackson Blvd Apt 407G Chicag"|'''),
        ('''|"Excalibur Investment Group|''', '''|"Excalibur Investment Group"|'''),
        ('''|"Advanced Metals Technology|''', '''|"Advanced Metals Technology"|'''),
        ('''|"Advantage Security|''', '''|"Advantage Security"|'''),
        ('''|"G. Brockman|''', '''|"G. Brockman"|'''),
        ('''|"I Factor|''', '''|"I Factor"|'''),
        ('''|"522 Highland Avenue |''', '''|"522 Highland Avenue"|'''),
        ('''|"AV-TECH INDUSTRIES|''', '''|"AV-TECH INDUSTRIES"|'''),  #
        ('''|Yard sign, stickers and website design"|''', '''|"Yard sign, stickers and website design"|'''),
        (
        '''|One Long Grove is a local political action committee which supports and opposes state, county and local political candidates and reports its business in detail to the Illinois State Board of Elections."|''',
        '''|"One Long Grove is a local political action committee which supports and opposes state, county and local political candidates and reports its business in detail to the Illinois State Board of Elections."|'''),
        ('''|Inc"|''', '''|"Inc"|'''),
        ('''|'AGGREGATE BELOW THRESHOLD"|''', '''|"AGGREGATE BELOW THRESHOLD"|'''),
        ('''|Fulfillment/Premium items (Ronald Reagan, Rendezvous With Destiny"|''',
         '''|"Fulfillment/Premium items (Ronald Reagan, Rendezvous With Destiny"|'''),
    )

    for m in malformed:
        line = line.replace(m[0], m[1])
    return line


@multi_asset(
    group_name="IRS_527",
    outs={
        "form8871_landing": AssetOut(io_manager_key="DuckPondIOManager"),
        "form8871_directors_landing": AssetOut(io_manager_key="DuckPondIOManager"),
        "form8871_related_entities_landing": AssetOut(io_manager_key="DuckPondIOManager"),
        "form8871_ein_landing": AssetOut(io_manager_key="DuckPondIOManager"),
        "form8872_landing": AssetOut(io_manager_key="DuckPondIOManager"),
        "form8872_schedule_a_landing": AssetOut(io_manager_key="DuckPondIOManager"),
        "form8872_schedule_b_landing": AssetOut(io_manager_key="DuckPondIOManager"),
    }
)
def clean_527_data(raw_527_data: Path, data_dictionary: dict):
    """
    Processes the raw_527_data file using the provided data_dictionary mappings for each form type.
    """
    logger = get_dagster_logger()
    records = defaultdict(list)

    with io.open(raw_527_data, 'r', encoding='ISO-8859-1') as raw_file:
        reader = csv.reader(map(fix_malformed_row, raw_file), delimiter='|')
        try:
            for i, row in enumerate(reader):
                if len(row) == 0: continue
                if row[0] in data_dictionary.keys():
                    process_row(previous_row, data_dictionary, records)
                    previous_row = row
                elif row[0] == 'H': previous_row = row
                elif row[0] == 'F': process_row(previous_row, data_dictionary, records)
                else:
                    previous_row = previous_row[:-1] + [previous_row[-1] + row[0]] + row[1:]
                if i % 500000 == 0:
                    logger.info(f"Processed {i / 500000}M rows processed so far.")
                    if i > 0: break
        except Exception as e:
            logger.error(f"Error processing {i}th row: {previous_row}")
            raise e

    return tuple(SQL('select * from $df', df=pd.DataFrame(records[key])) for key in ['1', 'D', 'R', 'E', '2', 'A', 'B'])


def load_sql_file(sql_file: Path):
    with open(sql_file, 'r') as f: return f.read()


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def form8872_contributors_staging(form8872_schedule_a_landing):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8872_contributors.sql"))
    return SQL(sql_template,
               form8872_schedule_a_landing=form8872_schedule_a_landing
               )

@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def form8872_contributions(form8872_schedule_a_landing, form8872_contributors_staging):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8872_contributions.sql"))
    return SQL(sql_template,
               form8872_schedule_a_landing=form8872_schedule_a_landing,
               form8872_contributors_staging=form8872_contributors_staging
               )

@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def form8872_recipients(form8872_schedule_b_landing):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8872_recipients.sql"))
    return SQL(sql_template,
               form8872_schedule_b_landing=form8872_schedule_b_landing,
               )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def form8872_expenditures(form8872_schedule_b_landing, form8872_recipients):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8872_expenditures.sql"))
    return SQL(sql_template,
               form8872_schedule_b_landing=form8872_schedule_b_landing,
               form8872_recipients=form8872_recipients
               )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def form8872(form8872_landing, addresses):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8872.sql"))
    return SQL(sql_template,
               form8872_landing=form8872_landing,
               addresses=addresses
               )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def addresses(form8871_landing, form8871_directors_landing, form8871_related_entities_landing, form8872_landing):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/addresses.sql"))
    return SQL(sql_template,
               form8871_landing=form8871_landing,
               form8871_directors_landing=form8871_directors_landing,
                form8871_related_entities_landing=form8871_related_entities_landing,
                form8872_landing=form8872_landing,
               )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def form8871(form8871_landing, addresses):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8871.sql"))
    return SQL(sql_template,
               form8871_landing=form8871_landing,
                addresses=addresses
               )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def form8871_ein(form8871_ein_landing):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8871_ein.sql"))
    return SQL(sql_template,
               form8871_ein_landing=form8871_ein_landing,
               )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def form8871_directors(form8871_directors_landing, addresses):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8871_directors.sql"))
    return SQL(sql_template,
               form8871_directors_landing=form8871_directors_landing,
                addresses=addresses
               )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def form8871_related_entities(form8871_related_entities_landing, addresses):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8871_related_entities.sql"))
    return SQL(sql_template,
               form8871_related_entities_landing=form8871_related_entities_landing,
                addresses=addresses
               )

# @asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
# def landing_cleanup(form8871, form8871_ein, form8871_directors,
#                     form8871_related_entities, form8872, form8872_contributions,
#                     form8872_expenditures):
#     sql_template = load_sql_file(sql_file=Path("datanym/assets/IRS527/sql_scripts/form8872_recipients.sql"))
#     return SQL(sql_template,
#                form8872_schedule_b_landing=form8872_schedule_b_landing,
#                )
