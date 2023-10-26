from airflow import DAG

from airflow.decorators import dag, task

from airflow.utils.dates import days_ago


from tempfile import NamedTemporaryFile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'email_on_retry': False,
}


with DAG(
    "branch_with_task",
    catchup=False,
    schedule_interval="@daily",
    tags=['me'],
    start_date=days_ago(3),
    max_active_runs=10,
):

    @task
    def _start():
        pass
    start = _start()

    @task.branch
    def _get_lead_data():
        accuracy = True
        if accuracy:
            return '_measure_data_quality'
        return '_no_upload'
    get_lead_data = _get_lead_data()

    @task
    def _measure_data_quality():
        pass
    measure_data_quality = _measure_data_quality()

    @task
    def _upload_clean_data_to_warehouse():
        pass
    upload_clean_data_to_warehouse = _upload_clean_data_to_warehouse()

    @task(trigger_rule='none_failed_min_one_success')
    def _end():
        pass
    end = _end()

    @task
    def _no_upload():
        pass
    no_upload = _no_upload()

    start >> get_lead_data >> [measure_data_quality, no_upload]
    measure_data_quality >> upload_clean_data_to_warehouse >> end
    no_upload >> end
