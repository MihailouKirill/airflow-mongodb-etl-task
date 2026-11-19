"""
Waits for the input archive to arrive, verifies that it is not empty, and branches into two paths:
1.Empty archive: logs a message and stops the pipeline.
2.Valid archive: extracts the CSV and runs three sequential transformation steps (filling nulls,
sorting by date, and cleaning text), updating the 'CLEANED_PATH' asset.
"""
from datetime import datetime
from typing import Any

from airflow.sdk import Asset, PokeReturnValue, chain, dag, task, task_group

from utils.config import CLEANED_PATH, PATH
from utils.extract import file_not_empty, read_csv_from_zip
from utils.transform import fill_nulls, remove_garbage, sort_data


@dag(
    dag_id = 'pipeline_dag',
    start_date = datetime(2000,11,16),
    schedule = None,
    catchup=False,
    doc_md= __doc__ ,
    )
def et_pipeline_dag():

    @task.sensor(task_id='exists_file_check',poke_interval = 5, timeout = 600, mode ='poke')
    def exists_file_check():
        if PATH.is_file():
            return PokeReturnValue(is_done = True,xcom_value = f'{PATH}')
        return PokeReturnValue(is_done = False)

    @task.branch(task_id = 'is_file_empty')
    def is_file_empty(data_path)-> Any:
        if file_not_empty(data_path):
                        return "extract_data"
        return "empty_data"
    @task.bash
    def empty_data():
        return "echo 'File is empty' "
    @task
    def extract_data(ti:Any = None):
        data_path = ti.xcom_pull(task_ids = 'exists_file_check')
        file_to_transform = read_csv_from_zip(data_path)
        return file_to_transform

    @task_group(group_id = 'transform_group',prefix_group_id=False)
    def transform_group(temp_data_path):
        @task(task_id ="data_without_null")
        def not_empty_data(tmp_data_path):
            result = fill_nulls(tmp_data_path)
            return result

        @task
        def data_sorting(tmp_data_path):
            sorted_data = sort_data(tmp_data_path)
            return str(sorted_data)

        @task(outlets = [Asset(str(CLEANED_PATH))])
        def data_garbage_remove(tmp_data_path):
            final_data = remove_garbage(tmp_data_path)

            return str(final_data)

        without_null = not_empty_data(temp_data_path)
        sorted_date = data_sorting(without_null)
        without_garbage = data_garbage_remove(sorted_date)


        return without_garbage

    zip_path = exists_file_check()
    branch = is_file_empty(zip_path)
    no_data = empty_data()
    extract = extract_data()
    chain(branch,[no_data,extract])
    transform_group(extract)



et_pipeline_dag()
