"""
Transformation module
"""
from pathlib import Path


def fill_nulls(data_path:str | Path) -> str:
    """
    Replaces null-values with '-' and loads into parquet

    :param data_path:path to temporary parquet file
    :return: path to temporary  parquet file
    """
    import pandas as pd
    file_to_transform = pd.read_parquet(Path(data_path))
    file_without_null = file_to_transform.fillna('-')

    file_without_null.to_parquet(data_path, index=False)

    return str(data_path)


def sort_data(temp_data_path:str | Path) -> str:
    """
    Sorts data by 'at'(created_date) using parquet + pyarrow to increase speed

    :param temp_data_path:path to parquet file
    :return: temporary parquet file path
    """
    import pandas as pd
    df_for_trans = pd.read_parquet(temp_data_path)
    sorted_df = df_for_trans.sort_values(by='at')
    sorted_df.to_parquet(temp_data_path, index=False)

    return str(temp_data_path)

def remove_garbage(temp_data_path:str | Path) -> str:
    """
    Removes all unnecessary characters from the content column (e.g. smiley faces, etc.),
    leaving only text and punctuation marks. And  changes format from parquet to csv

    :param temp_data_path:parquet file
    :return: ready csv file path
    """
    import pandas as pd
    df_for_trans = pd.read_parquet(temp_data_path)
    df_for_trans["content"] = (df_for_trans["content"].astype(str).str.
                               replace(r"[^\w\s.,!?;:'\"()-]", "", regex=True).str.
                               replace(r"\s+", " ", regex=True).str.strip())
    final_name = Path(temp_data_path).with_name('cleaned_data.csv')
    df_for_trans.to_csv(final_name, index=False)
    Path(temp_data_path).unlink(missing_ok=True)
    return str(final_name)


