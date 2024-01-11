import csv
import os.path
from collections import defaultdict
import io
from pathlib import Path
import pandas as pd
from .core import extract_file_from_zip, download_file, clean_extraction_directory
from ezduckdb import SQL

from dagster import (
    asset,
    AssetOut,
    multi_asset,
    get_dagster_logger,
)


@asset(group_name="IRS_527")  # , io_manager_key="local_io_manager")
def raw_527_data() -> Path:
    """
    Downloads the IRS 527 data zip file, extracts it, and prepares the data for processing.
    """

    url = "https://forms.irs.gov/app/pod/dataDownload/fullData"
    base_dir = Path("output_data/irs_527")

    zip_path = base_dir / "data.zip"
    extract_path = base_dir / "unzipped/"
    final_path = base_dir / "raw_FullDataFile.txt"

    download_file(url, zip_path)
    extract_file_from_zip(zip_path, extract_path)
    clean_extraction_directory(zip_path, extract_path, final_path)

    return final_path


@asset(group_name="IRS_527")  # , io_manager_key="local_io_manager")
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
        required_columns = ["position", "model_name", "field_type"]
        if not all(col in df.columns for col in required_columns):
            raise ValueError(
                f"Missing one of the required columns in record type {record_type}: {required_columns}"
            )

        # Build the mapping for the current record type
        mapping = {
            row["position"]: (row["model_name"], row["field_type"])
            for _, row in df.iterrows()
        }
        combined_mappings[record_type] = mapping

    return combined_mappings


def process_row(row: list, mappings: dict, records: dict) -> None:
    if row[0] == "H":
        return
    null_terms = [
        "N/A",
        "NOT APPLICABLE",
        "NA",
        "NONE",
        "NOT APPLICABE",
        "NOT APLICABLE",
        "N A",
        "N-A",
    ]
    mapping = mappings[row[0]]
    parsed_row = {}
    for i, cell in enumerate(row):
        cell = None if cell in null_terms else cell
        try:
            parsed_row[mapping[i][0]] = cell
        except KeyError as e:
            if cell == "":
                pass
            else:
                print(cell)
                raise e
    records[row[0]].writerow(parsed_row)


def fix_malformed_row(line: str) -> str:
    malformed = (
        ('|"I Factor|', '|"I Factor"|'),
        ("""|"N/A'|""", """|"N/A"|"""),
        ("""|"522 Highland Avenue |""", """|"522 Highland Avenue"|"""),
        ("""|"AV-TECH INDUSTRIES|""", """|"AV-TECH INDUSTRIES"|"""),
        ("""|"c/o Moxie Innovative|""", """|"c/o Moxie Innovative"|"""),
        ("""|"423 Georgia Ave SE |""", """|"423 Georgia Ave SE"|"""),
        ("""|"Chris Andrews|""", """|"Chris Andrews"|"""),
        ("""|"Petroleum|""", """|"Petroleum"|"""),
        ("""|"J. Conly and Associates|""", """|"J. Conly and Associates"|"""),
        ("""|"Ubs Securities|""", """|"Ubs Securities"|"""),
        ("""|"P.O. Box 619616 |""", """|"P.O. Box 619616"|"""),
        ("""|"WF Enterprises|""", """|"WF Enterprises"|"""),
        (
            """|"225 Broadway Ste 1410 New York, NY|""",
            """|"225 Broadway Ste 1410 New York, NY"|""",
        ),
        (
            """|"701 W Jackson Blvd Apt 407G Chicag|""",
            """|"701 W Jackson Blvd Apt 407G Chicag"|""",
        ),
        ("""|"Excalibur Investment Group|""", """|"Excalibur Investment Group"|"""),
        ("""|"Advanced Metals Technology|""", """|"Advanced Metals Technology"|"""),
        ("""|"Advantage Security|""", """|"Advantage Security"|"""),
        ("""|"G. Brockman|""", """|"G. Brockman"|"""),
        ("""|"I Factor|""", """|"I Factor"|"""),
        ("""|"522 Highland Avenue |""", """|"522 Highland Avenue"|"""),
        ("""|"AV-TECH INDUSTRIES|""", """|"AV-TECH INDUSTRIES"|"""),  #
        (
            """|Yard sign, stickers and website design"|""",
            """|"Yard sign, stickers and website design"|""",
        ),
        (
            """|One Long Grove is a local political action committee which supports and opposes state, county and local political candidates and reports its business in detail to the Illinois State Board of Elections."|""",
            """|"One Long Grove is a local political action committee which supports and opposes state, county and local political candidates and reports its business in detail to the Illinois State Board of Elections."|""",
        ),
        ("""|Inc"|""", """|"Inc"|"""),
        ("""|'AGGREGATE BELOW THRESHOLD"|""", """|"AGGREGATE BELOW THRESHOLD"|"""),
        (
            """|Fulfillment/Premium items (Ronald Reagan, Rendezvous With Destiny"|""",
            """|"Fulfillment/Premium items (Ronald Reagan, Rendezvous With Destiny"|""",
        ),
    )

    for m in malformed:
        line = line.replace(m[0], m[1])
    return line


@multi_asset(
    group_name="IRS_527",
    outs={
        "landing_form8871": AssetOut(io_manager_key="DuckPondIOManager"),
        "landing_form8871_directors": AssetOut(io_manager_key="DuckPondIOManager"),
        "landing_form8871_related_entities": AssetOut(
            io_manager_key="DuckPondIOManager"
        ),
        "landing_form8871_eain": AssetOut(io_manager_key="DuckPondIOManager"),
        "landing_form8872": AssetOut(io_manager_key="DuckPondIOManager"),
        "landing_form8872_schedule_a": AssetOut(io_manager_key="DuckPondIOManager"),
        "landing_form8872_schedule_b": AssetOut(io_manager_key="DuckPondIOManager"),
    },
)
def clean_527_data(raw_527_data: Path, data_dictionary: dict):
    """
    Processes the raw_527_data file using the provided data_dictionary mappings for each form type.
    """

    logger = get_dagster_logger()

    logger.info(f"Setup for processing raw 527 data file")
    outfiles, writers = {}, {}
    fieldnames = defaultdict(list)

    for key in ["1", "D", "R", "E", "2", "A", "B"]:
        if os.path.exists(f"temp_{key}.csv"):
            os.remove(f"temp_{key}.csv")
        outfiles[key] = open(f"temp_{key}.csv", "a")

        for position in range(0, len(data_dictionary[key].keys())):
            fieldnames[key].append(data_dictionary[key][position][0])
        writers[key] = csv.DictWriter(outfiles[key], fieldnames=fieldnames[key])
        writers[key].writeheader()

    logger.info(f"Processing raw 527 data file")
    with io.open(raw_527_data, "r", encoding="ISO-8859-1") as raw_file:
        reader = csv.reader(map(fix_malformed_row, raw_file), delimiter="|")
        try:
            for i, row in enumerate(reader):
                if len(row) == 0:
                    continue
                if row[0] in data_dictionary.keys():
                    process_row(previous_row, data_dictionary, writers)
                    previous_row = row
                elif row[0] == "H":
                    previous_row = row
                elif row[0] == "F":
                    process_row(previous_row, data_dictionary, writers)
                else:
                    previous_row = (
                            previous_row[:-1] + [previous_row[-1] + row[0]] + row[1:]
                    )
                if i % 250000 == 0:
                    logger.info(f"Processed {i / 1e6}M rows processed so far.")
                    # if i > 0:
                    #     break
        except Exception as e:
            logger.error(f"Error processing {i}th row: {previous_row}")
            raise e

    logger.info(f"Closing raw 527 data files")
    for key in ["1", "D", "R", "E", "2", "A", "B"]:
        outfiles[key].close()

    logger.info(f"Finished processing raw 527 data file")
    return tuple(
        SQL(
            "select * from read_csv_auto($csv, header=true, all_varchar=true)",
            csv=f"temp_{key}.csv",
        )
        for key in ["1", "D", "R", "E", "2", "A", "B"]
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_schedule_b_1cast(
        landing_form8872_schedule_b,
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8872_schedule_b_1cast.sql",
        **locals(),
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_schedule_b_2address(
        staging_form8872_schedule_b_1cast, curated_addresses
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8872_schedule_b_2address.sql",
        **locals(),
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_schedule_a_1cast(
        landing_form8872_schedule_a,
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8872_schedule_a_1cast.sql",
        **locals(),
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_schedule_a_2address(
        staging_form8872_schedule_a_1cast, curated_addresses
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8872_schedule_a_2address.sql",
        **locals(),
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_1cast(
        landing_form8872,
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8872_1cast.sql", **locals()
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_2address(staging_form8872_1cast, curated_addresses):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8872_2address.sql", **locals()
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8871_eain_1cast(
        landing_form8871_eain,
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8871_eain_1cast.sql", **locals()
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8871_related_entities_1cast(
        landing_form8871_related_entities,
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8871_related_entities_1cast.sql",
        **locals(),
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8871_related_entities_2address(
        staging_form8871_related_entities_1cast, curated_addresses
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8871_related_entities_2address.sql",
        **locals(),
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8871_directors_1cast(
        landing_form8871_directors,
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8871_directors_1cast.sql",
        **locals(),
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8871_directors_2address(
        staging_form8871_directors_1cast, curated_addresses
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8871_directors_2address.sql",
        **locals(),
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8871_1cast(
        landing_form8871,
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8871_1cast.sql", **locals()
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8871_2address(staging_form8871_1cast, curated_addresses):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/staging_form8871_2address.sql", **locals()
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_addresses(
        staging_form8871_1cast,
        staging_form8871_directors_1cast,
        staging_form8871_related_entities_1cast,
        staging_form8872_1cast,
        staging_form8872_schedule_a_1cast,
        staging_form8872_schedule_b_1cast,
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/curated_addresses.sql", **locals()
    )


@asset(group_name="IRS_527_DEV", io_manager_key="DuckPondIOManager")
def curated_eins(
        staging_form8871_1cast,
        staging_form8872_1cast,
):
    return SQL.from_file(
        "datanym/assets/IRS527/sql_scripts/curated_eins.sql", **locals()
    )


@asset(group_name="IRS_527", io_manager_key="duckDB_creator_io_manager")
def all_assets_to_duckdb(
        landing_form8871,
        landing_form8871_directors,
        landing_form8871_related_entities,
        landing_form8871_eain,
        landing_form8872,
        landing_form8872_schedule_a,
        landing_form8872_schedule_b,
        staging_form8871_1cast,
        staging_form8871_2address,
        staging_form8871_directors_1cast,
        staging_form8871_directors_2address,
        staging_form8871_related_entities_1cast,
        staging_form8871_related_entities_2address,
        staging_form8871_eain_1cast,
        staging_form8872_1cast,
        staging_form8872_2address,
        staging_form8872_schedule_a_1cast,
        staging_form8872_schedule_a_2address,
        staging_form8872_schedule_b_1cast,
        staging_form8872_schedule_b_2address,
        curated_addresses,
        curated_eins,
):
    return (
        landing_form8871,
        landing_form8871_directors,
        landing_form8871_related_entities,
        landing_form8871_eain,
        landing_form8872,
        landing_form8872_schedule_a,
        landing_form8872_schedule_b,
        staging_form8871_1cast,
        staging_form8871_2address,
        staging_form8871_directors_1cast,
        staging_form8871_directors_2address,
        staging_form8871_related_entities_1cast,
        staging_form8871_related_entities_2address,
        staging_form8871_eain_1cast,
        staging_form8872_1cast,
        staging_form8872_2address,
        staging_form8872_schedule_a_1cast,
        staging_form8872_schedule_a_2address,
        staging_form8872_schedule_b_1cast,
        staging_form8872_schedule_b_2address,
        # curated_form8871,
        # curated_form8871_directors,
        # curated_form8871_related_entities,
        # curated_form8871_eains,
        # curated_form8872,
        curated_addresses,
        curated_eins,
    )


@asset(group_name="IRS_527_DISTRIBUTE", io_manager_key="local_to_hf_io_manager")
def duckdb_to_hf(all_assets_to_duckdb):
    return (
        all_assets_to_duckdb,
        Path("Codenym/ISR-527-Political-Non-Profits/527_orgs.duckdb"),
    )
