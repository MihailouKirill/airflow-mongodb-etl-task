"""
Checks if the file is empty or not and read csv from zip
"""
import zipfile
from pathlib import Path


def file_not_empty(data_path:str | Path)->bool:
    """
    Checks if the file is empty or not.

    :param data_path: Path to the file to be checked
    :return:True if the file isn't empty, false otherwise
    """
    path = Path(data_path)
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path, 'r') as zf:
            for item in zf.infolist():
                if not item.is_dir() and item.file_size > 0:
                    return True
    return False

def read_csv_from_zip(data_path:str | Path)->str:
    """
    Searches for csv files in the zip file. And unpacks csv into temporary parquet file.

    :param data_path: Path to the zip file
    :return: path to the temporary parquet file
    """
    import pandas as pd

    with zipfile.ZipFile(data_path, 'r') as zf:
        name = next((f for f in zf.namelist() if f.lower().endswith('.csv')), None)

        if name is None:
            raise ValueError("No CSV file found in archive")

        with zf.open(name, 'r') as zip_reader:
            file_to_transform = pd.read_csv(zip_reader)

            tmp = Path(data_path).with_suffix('.tmp.parquet')
            file_to_transform.to_parquet(tmp, index=False)

        return str(tmp)