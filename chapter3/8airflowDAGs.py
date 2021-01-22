from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(dag_id = 'sample',
          ...,
          schedule_interval = "0 0 * * *")

etl_task  = PythonOperator(task_id = 'etl_task',
                           python_callable = etl,
                           dag = dag)

etl_task.set_upstream(wait_for_this_task)