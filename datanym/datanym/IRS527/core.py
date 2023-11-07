import requests
from pathlib import Path
import zipfile
import logging
import shutil
import os


def extract_file_from_zip(zip_path: Path, extract_path: Path) -> None:
    """
    Extracts the first file from a ZIP archive into a specified directory.

    :param zip_path: A Path object representing the ZIP archive file.
    :param extract_path: A Path object representing the target directory to extract the file.
    """

    if not extract_path.exists():
        extract_path.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zipped_archive:
        data_file = zipped_archive.namelist()[0]
        zipped_archive.extract(data_file, extract_path)


def download_file(url: str, dl_fname: Path) -> None:
    """
    Downloads a file from a given URL to a specified local file path, creating any new directories as needed.

   :param url: The URL of the file to download. Must be a non-empty string.
   :param dl_fname: Path to where the downloaded file will be saved, including file name
    """
    # Validate the URL
    if not isinstance(url, str) or not url:
        raise ValueError("The URL must be a non-empty string.")

    # Ensure dl_fname is a PATH
    if not isinstance(dl_fname, Path):
        raise TypeError("The download file name must be a Path.")

    dl_fname.parent.mkdir(parents=True, exist_ok=True)  # Create parent directories if necessary

    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code

    with open(dl_fname, 'wb') as f:
        for chunk in response.iter_content(chunk_size=30720):
            f.write(chunk)



def clean_extraction_directory(zip_path: Path, extract_path: Path, final_path: Path) -> None:
    """
    Cleans up directories after data extraction by moving a specific data file to a final destination and deleting
        the extraction directory and original zip file.

    :param Path zip_path: The path to the original zip file.
    :param Path extract_path: The path to the extraction directory where the zip file was initially extracted to.
    :param Path final_path: The target path where the data file should be moved to.
    """

    source_file = extract_path / "var" / "IRS" / "data" / "scripts" / "pofd" / "download" / "FullDataFile.txt"

    # Move data file to the final path
    if not final_path.parent.exists():
        final_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_file), str(final_path))

    # Remove the extraction directory
    if extract_path.exists():
        shutil.rmtree(str(extract_path))

    # Remove the original zip file
    if zip_path.exists():
        os.remove(str(zip_path))



