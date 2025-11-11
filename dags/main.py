from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_file
from dataquality.soda import yt_elt_data_quality

from datawarehouse.dwh import staging_table, core_table

# Define the local timezone
local_tz = pendulum.timezone("Africa/Lagos")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with extracated data",
    schedule="0 14 * * *",
    catchup=False
) as dag_produce:
    plalist_id = get_playlist_id()
    video_ids = get_video_ids(plalist_id)
    extracted_data = extract_video_data(video_ids)
    saved_json = save_file(extracted_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id='trigger_update_db',
        trigger_dag_id='update_db',
    )

    plalist_id >> video_ids >> extracted_data >> saved_json >> trigger_update_db


with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON into staging and core schemas",
    schedule=None,
    catchup=False
) as dag_update:
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id='trigger_data_quality',
        trigger_dag_id='data_quality',
    )

    update_staging >> update_core >> trigger_data_quality

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check data quality on both staging and core tables",
    schedule=None,
    catchup=False
) as dag_quality:
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    soda_validate_staging >> soda_validate_core