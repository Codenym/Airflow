import requests
from pathlib import Path
import zipfile
import logging
import shutil
import os


def extract_file_from_zip(zip_path: Path, extract_path: Path):
    """
    Extracts the first file from a zip archive to a specified path.

    Parameters:
    - zip_path: A pathlib.Path to the zip file.
    - extract_path: A pathlib.Path where the file will be extracted to.

    Returns:
    - A boolean indicating if the extraction was successful.

    Raises:
    - FileNotFoundError: If the zip file does not exist.
    - zipfile.BadZipFile: If the zip file is corrupt or otherwise unreadable.
    - KeyError: If the specified file to extract is not found within the zip file.

    Example usage:
    - success = extract_file_from_zip(Path("/path/to/zipfile.zip"), Path("/path/to/extracted/file"))
    """
    if not zip_path.is_file():
        raise FileNotFoundError(f"The zip file {zip_path} does not exist.")

    if not extract_path.exists():
        extract_path.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path, 'r') as zipped_archive:
            data_file = zipped_archive.namelist()[0]
            zipped_archive.extract(data_file, extract_path)
    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(f"The file {zip_path} is not a valid zip archive.") from e
    except KeyError as e:
        raise KeyError(f"The file {data_file} is not found in the zip archive.") from e

    return True


def download_file(url: str, dl_fname: Path) -> None:
    """
    Downloads a file from the given URL to a local path.

    Parameters:
    url (str): The URL from which to download the file.
    dl_fname (str): The local file path to save the downloaded file.

    Raises:
    ValueError: If the URL is invalid or the download fails.
    IOError: If there are issues writing the file to the disk.
    """
    # Validate the URL
    if not isinstance(url, str) or not url:
        raise ValueError("The URL must be a non-empty string.")

    # Ensure dl_fname is a PATH
    if not isinstance(dl_fname, Path):
        raise TypeError("The download file name must be a Path.")

    dl_fname.parent.mkdir(parents=True, exist_ok=True)  # Create parent directories if necessary

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code

        with open(dl_fname, 'wb') as f:
            for chunk in response.iter_content(chunk_size=30720):
                f.write(chunk)
    except requests.RequestException as e:
        raise ValueError(f"Failed to download the file: {e}")
    except IOError as e:
        raise IOError(f"Failed to save the file: {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}")


def clean_extraction_directory(zip_path: Path, extract_path: Path, final_path: Path):
    """
    Move the extracted data file to a final location, then clean up the extraction directory and original zip file.

    :param zip_path: A Path object pointing to the zip file to be removed.
    :param extract_path: A Path object pointing to the extraction directory to be cleaned up.
    :param final_path: A Path object pointing to the final destination of the data file.
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

   # zip_path = (base_dir / 'data.zip')
   #  extract_path = (base_dir / 'unzipped/')
   #  final_path = (base_dir / 'raw_FullDataFile.txt')


