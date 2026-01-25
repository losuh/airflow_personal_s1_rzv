import shutil

import pendulum
import os
import zipfile
import re

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

SHARED_BASE_DIR = "/opt/airflow/shared_workers_data"
BUCKET_NAME_SOURCE = "citibike-data"
BUCKET_NAME_TARGET = "123321test"
CSV_RE = re.compile(r"\d{6}-citibike-tripdata(_\d+)?\.csv$")


def check_if_month_exists(file_name, hook, bucket_name) -> bool:
    """Check if target month data exists in bucket"""
    s3_hook = hook
    response = s3_hook.check_for_key(key=file_name, bucket_name=bucket_name)

    return response


default_args = {
    'group': 'g4',
	'owner': 'Daniil Konovalov',
    'retries': 3,
    'catchup' : True,
}

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
def xz():
    @task
    def extract():
        context = get_current_context()
        execution_date = context["ds_nodash"][:6]
        file_name = f"{execution_date}-citibike-tripdata.zip"

        s3_hook_yandex = S3Hook(aws_conn_id="yandex_s3")

        check_if_month_exists(file_name, s3_hook_yandex, BUCKET_NAME_SOURCE)

        downloaded_path = s3_hook_yandex.download_file(
            key=file_name,
            bucket_name=BUCKET_NAME_SOURCE,
            local_path=SHARED_BASE_DIR,
        )
        print(execution_date[:6])
        return {
            "downloaded_path": downloaded_path,
            "file_name": file_name,
        }

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
    def load(extract_dir):
        csv_files = []

        for root, _, files in os.walk(extract_dir):
            for f in files:
                if CSV_RE.match(f):
                    csv_files.append(os.path.join(root, f))

        s3_hook_target = S3Hook(aws_conn_id='minio')
        context = get_current_context()
        execution_date = context["ds_nodash"][:6]
        keys = []

        for idx, file in enumerate(sorted(csv_files)):
            part = f"{idx:02d}"
            key = f"raw/citibike_data/{execution_date}/{execution_date}-citibike-tripdata-part{part}.csv"

            if check_if_month_exists(key, s3_hook_target, BUCKET_NAME_TARGET):
                print(f"Файл {file} уже загружен")

            else:
                s3_hook_target.load_file(
                    filename=file,
                    key=key,
                    bucket_name=BUCKET_NAME_TARGET,
                )
                keys.append(key)


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

    data = extract()
    extract_root = unzip(data)
    load_task = load(extract_root)
    load_task >> cleanup(data, extract_root)

xz()

