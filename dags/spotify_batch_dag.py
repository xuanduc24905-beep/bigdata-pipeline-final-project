from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'spotify_batch_pipeline',
    default_args=default_args,
    description='Lambda Architecture - Batch Layer',
    schedule_interval='@once',
    start_date=days_ago(1),
    tags=['spotify', 'batch'],
    catchup=False
) as dag:

    # Khai báo các biến bash command thay vì lặp lại
    SPARK = "docker exec spark-master bash -c"
    SPARK_SUBMIT = "spark-submit --master spark://spark-master:7077"

    # Task 1
    load_csv = BashOperator(
        task_id='load_csv',
        bash_command='docker exec spark-master python /spark-jobs/00_load_csv.py',
    )

    # Task 2
    upload_hdfs = BashOperator(
        task_id='upload_hdfs',
        bash_command='docker exec namenode bash -c "hdfs dfs -mkdir -p /music/raw && hdfs dfs -put -f /data/spotify_tracks_cleaned.csv /music/raw/spotify_tracks.csv"',
    )

    # Task 3
    spark_eda = BashOperator(
        task_id='spark_eda',
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/01_eda.py"',
    )

    # Task 4
    spark_kmeans = BashOperator(
        task_id='spark_kmeans',
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/02_kmeans.py"',
    )

    # Task 5
    hive_queries = BashOperator(
        task_id='hive_queries',
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/04_hive_query.py"',
    )

    # Task 6
    export_parquet = BashOperator(
        task_id='export_parquet',
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/05_export.py"',
    )

    # Định nghĩa luồng phụ thuộc (flow graph)
    load_csv >> upload_hdfs >> spark_eda >> spark_kmeans >> hive_queries >> export_parquet
