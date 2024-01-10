import csv
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


@asset(group_name="IRS_527_LANDING", io_manager_key="local_io_manager")
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


@asset(group_name="IRS_527_LANDING", io_manager_key="local_io_manager")
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
    records[row[0]].append(parsed_row)


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
    group_name="IRS_527_LANDING",
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
    records = defaultdict(list)

    with io.open(raw_527_data, "r", encoding="ISO-8859-1") as raw_file:
        reader = csv.reader(map(fix_malformed_row, raw_file), delimiter="|")
        try:
            for i, row in enumerate(reader):
                if len(row) == 0:
                    continue
                if row[0] in data_dictionary.keys():
                    process_row(previous_row, data_dictionary, records)
                    previous_row = row
                elif row[0] == "H":
                    previous_row = row
                elif row[0] == "F":
                    process_row(previous_row, data_dictionary, records)
                else:
                    previous_row = (
                            previous_row[:-1] + [previous_row[-1] + row[0]] + row[1:]
                    )
                if i % 500000 == 0:
                    logger.info(f"Processed {i / 500000}M rows processed so far.")
                    # if i > 0:
                    #    break
        except Exception as e:
            logger.error(f"Error processing {i}th row: {previous_row}")
            raise e

    return tuple(
        SQL("select * from $df", df=pd.DataFrame(records[key]))
        for key in ["1", "D", "R", "E", "2", "A", "B"]
    )


@asset(group_name="IRS_527_LANDING", io_manager_key="duckDB_creator_io_manager")
def landing_assets(
        landing_form8871,
        landing_form8871_directors,
        landing_form8871_related_entities,
        landing_form8871_eain,
        landing_form8872,
        landing_form8872_schedule_a,
        landing_form8872_schedule_b,
):
    return (
        landing_form8871,
        landing_form8871_directors,
        landing_form8871_related_entities,
        landing_form8871_eain,
        landing_form8872,
        landing_form8872_schedule_a,
        landing_form8872_schedule_b,
    )


def load_sql_file(sql_file: Path):
    with open(sql_file, "r") as f:
        return f.read()


@asset(group_name="IRS_527_STAGING1", io_manager_key="DuckPondIOManager")
def staging_form8872_schedule_b(
        landing_form8872_schedule_b,
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8872_schedule_b.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8872_schedule_b=landing_form8872_schedule_b,
    )
@asset(group_name="IRS_527_STAGING1", io_manager_key="DuckPondIOManager")
def staging_form8872_schedule_a(
        landing_form8872_schedule_a,
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8872_schedule_a.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8872_schedule_a=landing_form8872_schedule_a,
    )

@asset(group_name="IRS_527_STAGING1", io_manager_key="DuckPondIOManager")
def staging_form8872(
        landing_form8872,
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8872.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8872=landing_form8872,
    )

@asset(group_name="IRS_527_STAGING1", io_manager_key="DuckPondIOManager")
def staging_form8871_eain(
        landing_form8871_eain,
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8871_eain.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8871_eain=landing_form8871_eain,
    )
@asset(group_name="IRS_527_STAGING1", io_manager_key="DuckPondIOManager")
def staging_form8871_related_entities(
        landing_form8871_related_entities,
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8871_related_entities.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8871_related_entities=landing_form8871_related_entities,
    )


@asset(group_name="IRS_527_STAGING1", io_manager_key="DuckPondIOManager")
def staging_form8871_directors(
        landing_form8871_directors,
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8871_directors.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8871_directors=landing_form8871_directors,
    )

@asset(group_name="IRS_527_STAGING1", io_manager_key="DuckPondIOManager")
def staging_form8871(
        landing_form8871,
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8871.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8871=landing_form8871,
    )

@asset(group_name="IRS_527_STAGING1", io_manager_key="duckDB_creator_io_manager")
def staging1_assets(
        landing_form8871,
        landing_form8871_directors,
        landing_form8871_related_entities,
        landing_form8871_eain,
        landing_form8872,
        landing_form8872_schedule_a,
        landing_form8872_schedule_b,
        staging_form8871,
        staging_form8871_directors,
        staging_form8871_related_entities,
        staging_form8871_eain,
        staging_form8872,
        staging_form8872_schedule_a,
        staging_form8872_schedule_b,
):
    return (
        landing_form8871,
        landing_form8871_directors,
        landing_form8871_related_entities,
        landing_form8871_eain,
        landing_form8872,
        landing_form8872_schedule_a,
        landing_form8872_schedule_b,
        staging_form8871,
        staging_form8871_directors,
        staging_form8871_related_entities,
        staging_form8871_eain,
        staging_form8872,
        staging_form8872_schedule_a,
        staging_form8872_schedule_b,
    )














@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_entities_schedule_a(
        landing_form8872_schedule_a,
        curated_addresses,
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8872_entities_schedule_a.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8872_schedule_a=landing_form8872_schedule_a,
        curated_addresses=curated_addresses,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_entities_schedule_b(
        curated_addresses, landing_form8872_schedule_b
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8872_entities_schedule_b.sql"
        )
    )
    return SQL(
        sql_template,
        curated_addresses=curated_addresses,
        landing_form8872_schedule_b=landing_form8872_schedule_b,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_form8872_entities(
        staging_form8872_entities_schedule_a,
        staging_form8872_entities_schedule_b,
):
    sql_template = load_sql_file(
        sql_file=Path("datanym/assets/IRS527/sql_scripts/curated_form8872_entities.sql")
    )
    return SQL(
        sql_template,
        staging_form8872_entities_schedule_a=staging_form8872_entities_schedule_a,
        staging_form8872_entities_schedule_b=staging_form8872_entities_schedule_b,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_form8872_transactions(
        staging_form8872_contributions, staging_form8872_expenditures
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/curated_form8872_transactions.sql"
        )
    )
    return SQL(
        sql_template,
        staging_form8872_contributions=staging_form8872_contributions,
        staging_form8872_expenditures=staging_form8872_expenditures,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_contributions(
        landing_form8872_schedule_a, curated_form8872_entities, curated_addresses
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8872_contributions.sql"
        )
    )
    return SQL(
        sql_template,
        curated_form8872_entities=curated_form8872_entities,
        curated_addresses=curated_addresses,
        landing_form8872_schedule_a=landing_form8872_schedule_a,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def staging_form8872_expenditures(
        landing_form8872_schedule_b,
        curated_form8872_entities,
        curated_addresses,
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/staging_form8872_expenditures.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8872_schedule_b=landing_form8872_schedule_b,
        curated_form8872_entities=curated_form8872_entities,
        curated_addresses=curated_addresses,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_form8872(landing_form8872, curated_addresses, curated_eins):
    sql_template = load_sql_file(
        sql_file=Path("datanym/assets/IRS527/sql_scripts/curated_form8872.sql")
    )
    return SQL(
        sql_template,
        landing_form8872=landing_form8872,
        curated_addresses=curated_addresses,
        curated_eins=curated_eins,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_addresses(
        landing_form8871,
        landing_form8871_directors,
        landing_form8871_related_entities,
        landing_form8872,
        landing_form8872_schedule_a,
        landing_form8872_schedule_b,
):
    sql_template = load_sql_file(
        sql_file=Path("datanym/assets/IRS527/sql_scripts/curated_addresses.sql")
    )
    return SQL(
        sql_template,
        landing_form8871=landing_form8871,
        landing_form8871_directors=landing_form8871_directors,
        landing_form8871_related_entities=landing_form8871_related_entities,
        landing_form8872=landing_form8872,
        landing_form8872_schedule_a=landing_form8872_schedule_a,
        landing_form8872_schedule_b=landing_form8872_schedule_b,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_eins(
        landing_form8871,
        landing_form8872,
        landing_form8871_directors,
        landing_form8871_related_entities,
):
    sql_template = load_sql_file(
        sql_file=Path("datanym/assets/IRS527/sql_scripts/curated_eins.sql")
    )
    return SQL(
        sql_template,
        landing_form8871=landing_form8871,
        landing_form8872=landing_form8872,
        landing_form8871_directors=landing_form8871_directors,
        landing_form8871_related_entities=landing_form8871_related_entities,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_form8871(landing_form8871, curated_addresses, curated_eins):
    sql_template = load_sql_file(
        sql_file=Path("datanym/assets/IRS527/sql_scripts/curated_form8871.sql")
    )
    return SQL(
        sql_template,
        landing_form8871=landing_form8871,
        curated_addresses=curated_addresses,
        curated_eins=curated_eins,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_form8871_eains(landing_form8871_eain):
    sql_template = load_sql_file(
        sql_file=Path("datanym/assets/IRS527/sql_scripts/curated_form8871_eains.sql")
    )
    return SQL(
        sql_template,
        landing_form8871_eain=landing_form8871_eain,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_form8871_directors(
        landing_form8871_directors, curated_addresses, curated_eins
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/curated_form8871_directors.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8871_directors=landing_form8871_directors,
        curated_addresses=curated_addresses,
        curated_eins=curated_eins,
    )


@asset(group_name="IRS_527", io_manager_key="DuckPondIOManager")
def curated_form8871_related_entities(
        landing_form8871_related_entities, curated_addresses, curated_eins
):
    sql_template = load_sql_file(
        sql_file=Path(
            "datanym/assets/IRS527/sql_scripts/curated_form8871_related_entities.sql"
        )
    )
    return SQL(
        sql_template,
        landing_form8871_related_entities=landing_form8871_related_entities,
        curated_addresses=curated_addresses,
        curated_eins=curated_eins,
    )


@asset(group_name="IRS_527", io_manager_key="duckDB_creator_io_manager")
def all_assets_to_duckdb(
        curated_form8871,
        curated_form8871_directors,
        curated_form8871_related_entities,
        curated_form8871_eains,
        curated_form8872,
        curated_form8872_entities,
        curated_form8872_transactions,
        staging_form8872_contributions,
        staging_form8872_expenditures,
        curated_addresses,
        curated_eins,
        landing_form8871,
        landing_form8871_directors,
        landing_form8871_related_entities,
        landing_form8871_eain,
        landing_form8872,
        landing_form8872_schedule_a,
        landing_form8872_schedule_b,
):
    return (
        curated_form8871,
        curated_form8871_directors,
        curated_form8871_related_entities,
        curated_form8871_eains,
        curated_form8872,
        curated_form8872_entities,
        curated_form8872_transactions,
        staging_form8872_contributions,
        staging_form8872_expenditures,
        curated_addresses,
        curated_eins,
        landing_form8871,
        landing_form8871_directors,
        landing_form8871_related_entities,
        landing_form8871_eain,
        landing_form8872,
        landing_form8872_schedule_a,
        landing_form8872_schedule_b,
    )


@asset(group_name="IRS_527", io_manager_key="local_to_hf_io_manager")
def duckdb_to_hf(all_assets_to_duckdb):
    return (
        all_assets_to_duckdb,
        Path("Codenym/ISR-527-Political-Non-Profits/527_orgs.duckdb"),
    )
