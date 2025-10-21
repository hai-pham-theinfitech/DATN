from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import pendulum

# DAG 1
with DAG(
    dag_id="upsert_master_vietnamworks",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
) as dag1:
    run_scrapy_task1 = BashOperator(
        task_id="crawl",    
        bash_command="cd /opt/airflow/crawler && python -m vietnamworks.upsert_master_companies"
    )

# DAG 2
with DAG(
    dag_id="upsert_master_recruit_vietnamworks",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
) as dag2:
    run_scrapy_task2 = BashOperator(
        task_id="crawl",    
        bash_command="cd /opt/airflow/crawler && python -m vietnamworks.upsert_recruit"
    )

# Main DAG to trigger DAGs in sequence
with DAG(
    dag_id="upsert_master_full_vietnamworks",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
) as main_dag:
    trigger_dag1 = TriggerDagRunOperator(
        task_id="trigger_dag1", 
        trigger_dag_id="upsert_master_vietnamworks"
    )
    
    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_dag2", 
        trigger_dag_id="upsert_master_recruit_vietnamworks"
    )

    trigger_dag1 >> trigger_dag2
