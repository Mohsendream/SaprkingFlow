import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="spark_airflow",
    default_args={
        "owner": "Madiouni Mohsen",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("jobs started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark_conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)
end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("jobs completed successfully"),
    dag=dag
)

start >> python_job >> end
