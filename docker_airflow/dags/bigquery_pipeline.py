# Import packages
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'Abar Arunachalam',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define dag
dag = DAG('bigquery_pipeline',
          start_date=datetime.now() - timedelta(days=1),
          schedule_interval='@once',
          concurrency=1,
          max_active_runs=1,
          template_searchpath = ['/usr/local/airflow/scripts_load_bq_tables'],
          default_args=default_args)

start_pipeline = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag
)

daily_load_raw_data_dedup = BigQueryOperator(
    task_id = 'daily_load_raw_data_dedup',
    use_legacy_sql = False,
    sql = 'rawdata_dedup.sql'
)

daily_load_region_dim = BigQueryOperator(
    task_id = 'daily_load_region_dim',
    use_legacy_sql = False,
    sql = 'load_region_dim.sql'
)

daily_load_fact_country_covid_metrics = BigQueryOperator(
    task_id = 'daily_load_fact_country_covid_metrics',
    use_legacy_sql = False,
    sql = 'load_fact_daily_country_covid_metrics.sql'
)

daily_load_fact_covid_cluster_analysis = BigQueryOperator(
    task_id = 'daily_load_fact_covid_cluster_analysis',
    use_legacy_sql = False,
    sql = 'load_fact_covid_cluster_analysis.sql'
)

finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline',
    dag = dag
)

# Define bigquery task dependencies
dag >> \
  start_pipeline >> \
    daily_load_raw_data_dedup >> daily_load_region_dim >> \
    daily_load_fact_country_covid_metrics >> \
    daily_load_fact_covid_cluster_analysis >> \
  finish_pipeline