from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow'
}

with DAG('databricks_dag',
         start_date=days_ago(2),
         schedule_interval=None,  # Set to None so it doesn't run on a schedule
         default_args=default_args
         ) as dag:

    opr_run_now = DatabricksRunNowOperator(
        task_id='task-job',
        databricks_conn_id='databricks_default',
        job_id='805689785554479',  # Note: Job ID should be provided as a string
    )