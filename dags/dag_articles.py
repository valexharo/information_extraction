from __future__ import print_function


from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from get_url_data import New

args = {
    'owner': 'Valeria',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='process_articles',
    default_args=args,
    schedule_interval='@daily',
    tags=['example']
)

start = DummyOperator(
        task_id='Start',
        dag=dag
    )
end = DummyOperator(
        task_id='End',
        dag=dag
    )

run_this = PythonOperator(
    task_id='get_content',
    provide_context=True,
    python_callable=New.processFile(),
    dag=dag,
)

start >> run_this >> end
