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


def check_if_month_already_loaded(file_name, hook) -> bool:
    """Check if target month data already loaded"""
    s3_hook = hook
    response = s3_hook.list_keys(bucket_name=BUCKET_NAME_TARGET)

    print(response)
    if file_name in response:
        return True
    else:
        return False

def check_if_month_exists(file_name, hook) -> bool:
    """Check if target month data exists"""
    s3_hook = hook
    response = s3_hook.list_keys(bucket_name=BUCKET_NAME_SOURCE)

    print(response)
    if file_name in response:
        return True
    else:
        raise AirflowSkipException(f"Файл {file_name} не найден в бакете!")



default_args = {
    'group': 'g4',
	'owner': 'Daniil Konovalov',
    'catchup' : True,
    'max_active_runs': 1,
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

        check_if_month_exists(file_name, s3_hook_yandex)

        downloaded_path = s3_hook_yandex.download_file(
            key=file_name,
            bucket_name="citibike-data",
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

        csv_files = []
        for root, _, files in os.walk(extract_dir):
            for f in files:
                if CSV_RE.match(f):
                    csv_files.append(os.path.join(root, f))

        return csv_files

    @task()
    def load(csv_files):
        s3_hook = S3Hook(aws_conn_id='minio')
        context = get_current_context()
        execution_date = context["ds_nodash"][:6]
        keys = []
        wrong_keys = []

        for idx, file in enumerate(sorted(csv_files)):
            part = f"{idx:02d}"
            key = f"raw/citibike_data/{execution_date}/{execution_date}-citibike-tripdata-part{part}.csv"
            if check_if_month_already_loaded(key, s3_hook):
                wrong_keys.append(key)
                print(f"Файл {file} уже загружен")
            else:
                s3_hook.load_file(
                    filename=file,
                    key=key,
                    bucket_name=BUCKET_NAME_TARGET
                )
                keys.append(key)


                print(f"Файл успешно загружен: {file} -> {key}")
        return

    @task()
    def cleanup(total_order_value: float):
        return

    data = extract()
    file_list = unzip(data)
    load(file_list)


xz()