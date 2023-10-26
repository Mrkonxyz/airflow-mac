from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from airflow.utils.dates import days_ago


from tempfile import NamedTemporaryFile
# from helpers.crm_transform.lead import clean_data, create_new_data_frame, get_start_and_end_of_day

# from utils import discord_notification

import pandas as pd


# MAILING_LIST = [Variable.get("mailing_list")]
# EMAIL_ON_FAILURE = Variable.get("email_on_failure")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    # 'email': MAILING_LIST,
    # 'email_on_failure': EMAIL_ON_FAILURE,
    'email_on_retry': False,
    # 'on_failure_callback': discord_notification
}


with DAG(
    "branch",
    catchup=False,
    schedule_interval="@daily",
    tags=['me'],
    start_date=days_ago(3),
    max_active_runs=10,
):

    start = EmptyOperator(task_id="start")

    def _get_lead_data():
        accuracy = 1
        if accuracy > 5:
            return 'measure_data_quality'
        return 'no_upload'

    get_lead_data = BranchPythonOperator(
        task_id='_get_lead_data',
        python_callable=_get_lead_data
    )

    measure_data_quality = EmptyOperator(
        task_id="measure_data_quality")

    no_upload = EmptyOperator(
        task_id="no_upload")

    upload_clean_data_to_warehouse = EmptyOperator(
        task_id="upload_clean_data_to_warehouse")

    end = EmptyOperator(
        task_id="task_end",
        trigger_rule='none_failed_min_one_success')

    start >> get_lead_data >> [measure_data_quality, no_upload]
    measure_data_quality >> upload_clean_data_to_warehouse >> end
    no_upload >> end
