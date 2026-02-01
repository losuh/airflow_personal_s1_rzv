import shutil
from logging import raiseExceptions

import pendulum
import os
import zipfile
import re

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.exceptions import AirflowSkipException

SHARED_BASE_DIR = "/opt/airflow/shared_workers_data"

BUCKET_NAME_SOURCE = "citibike-data"
BUCKET_NAME_TARGET = "123321test"

AWS_CONN_ID_SOURCE = "yandex_s3"
AWS_CONN_ID_TARGET = "minio"


CSV_RE = re.compile(r"\d{6}-citibike-tripdata(_\d+)?\.csv$")


def check_if_month_exists(file_name, list_keys) -> bool:
    """Check if target month data exists in bucket"""

    print(list_keys)
    response = file_name in list_keys

    return response


default_args = {
    'group': 'g4',
	'owner': 'Daniil Konovalov',
    'retries': 3,
    'catchup' : True,
}

@task
def get_date():
    context = get_current_context()
    execution_date = context["ds_nodash"][:6]
    return execution_date

@task
def extract(list_keys, execution_date):

    file_name = f"{execution_date}-citibike-tripdata.zip"

    s3_hook_yandex = S3Hook(aws_conn_id=AWS_CONN_ID_SOURCE)

    if check_if_month_exists(file_name, list_keys):

        downloaded_path = s3_hook_yandex.download_file(
            key=file_name,
            bucket_name=BUCKET_NAME_SOURCE,
            local_path=SHARED_BASE_DIR,
        )

        print(f"Скачан файл за {execution_date}")

        return {
            "downloaded_path": downloaded_path,
            "file_name": file_name,
        }

    else:
        raise AirflowSkipException(f"Нет данных в источнике за дату {execution_date}")

@task()
def unzip(data):

    zip_path = data["downloaded_path"]
    file_name = data["file_name"]

    base_dir = os.path.dirname(zip_path)

    extract_dir = os.path.join(
        base_dir,
        file_name.replace(".zip", "")
    )

    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_dir)

    return extract_dir

@task()
def load(extract_dir, list_keys, execution_date):

    csv_files = []

    for root, _, files in os.walk(extract_dir):
        for f in files:
            if CSV_RE.match(f):
                csv_files.append(os.path.join(root, f))

    s3_hook_target = S3Hook(aws_conn_id=AWS_CONN_ID_TARGET)

    for idx, file in enumerate(sorted(csv_files)):

        part = f"{idx:02d}"
        key = f"raw/citibike_data/{execution_date}/{execution_date}-citibike-tripdata-part{part}.csv"

        if check_if_month_exists(key, list_keys):
            print(f"Файл {file} уже загружен")
        else:
            s3_hook_target.load_file(
                filename=file,
                key=key,
                bucket_name=BUCKET_NAME_TARGET,
            )

            print(f"Файл успешно загружен: {file} -> {key}")

@task(trigger_rule="all_done")
def cleanup(data, dir_path):

    zip_path = data["downloaded_path"]

    for path in (dir_path, zip_path):

        if os.path.exists(path):

            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

            print(f"Удалён {path}")


@dag(
    dag_id='g4_konovalov_daniil_s1_dag',
    description='my first dag for s1',
    tags=["group:g4", "owner:konovalov.daniil", "stage:s1"],
    doc_md="some doc information bla bla bla",
    schedule="0 0 L * *",
    start_date=pendulum.datetime(2024, 12, 16, tz="Europe/Moscow"),
    max_active_runs=1,
    default_args=default_args,
)
def s3_dag():

    list_keys_source = S3ListOperator(
        task_id="list_keys_source",
        bucket=BUCKET_NAME_SOURCE,
        aws_conn_id=AWS_CONN_ID_SOURCE,
    )

    list_keys_target = S3ListOperator(
        task_id="list_keys_target",
        bucket=BUCKET_NAME_TARGET,
        aws_conn_id=AWS_CONN_ID_TARGET,
    )
    execution_date = get_date()
    data = extract(list_keys_source.output, execution_date)
    extract_root = unzip(data)
    load_task = load(extract_root, list_keys_target.output, execution_date)
    load_task >> cleanup(data, extract_root)

s3_dag()

