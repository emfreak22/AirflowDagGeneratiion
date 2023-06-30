from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def firstFunctionExecution(**context):
    context['ti'].xcom_push(key = 'mykey', value = 500)
    print("hello airflow")
    return f"Hello Airflow! by"

def seconfFunctionExecution(**context):
    data= context['ti'].xcom_pull(key= 'mykey')
    print(f'Data passed from first function is {data}')


with DAG(
        dag_id='first_dag',
        schedule_interval='@daily',
        default_args={
            'owner': 'airflow',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2020, 11, 11)
        },
            catchup= False
    ) as f:
    firstFunctionExecute = PythonOperator(task_id = 'firstFunctionExecution',
                                          python_callable = firstFunctionExecution,
                                          op_kwargs = {"Executor": 'User'}
                                          )
    secondFunctionExecute = PythonOperator(task_id = 'secondFunctionExecution',
                                           python_callable = seconfFunctionExecution
                                           )
firstFunctionExecute >> secondFunctionExecute