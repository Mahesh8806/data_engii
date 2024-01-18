import logging

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator



def hello_world():
    # logging.INFO("Run successfully...")
    print("hellow world")

arg = {
    'owner': 'Mahesh',
    'retries': 5,
    'retry_delay': timedelta(2)
}

dag = DAG(
        dag_id="ita-pin-maintenance",
        default_args=arg,
        schedule_interval=None,
        start_date = datetime(2023, 1, 1)
)


def print_num(num):
    print("passed args is : ", num)

def check(**kwargs):
    return ["run-func2"]


task1 = PythonOperator(
    dag= dag,
    task_id="run-func1",
    python_callable=hello_world,
    start_date=None
)


task2 = PythonOperator( dag= dag,
                        task_id="run-func2",
                        python_callable=print_num,
                        op_args=[2])

terminate = PythonOperator(dag= dag,
                        task_id="Terminate",
                        python_callable=print_num,
                        op_args=[0],
                        trigger_rule='one_success'                       
                        )

branching = BranchPythonOperator(dag=dag,
                                 task_id="CheckFlag",
                                 python_callable=check,
                                 trigger_rule='one_success' 
                                )

task3 = PythonOperator( dag= dag,
                        task_id="run-func3",
                        python_callable=print_num,
                        op_args=[3])

task4 = PythonOperator( dag= dag,
                        task_id="run-func4",
                        python_callable=print_num,
                        op_args=[4])

notify = PythonOperator( dag= dag,
                        task_id="Nofify",
                        python_callable=print_num,
                        op_args=["Dag Run Successfully"])


task1 >> branching >> [task2, task3]
task2 >> terminate
task3 >> task4 >> terminate 
terminate >> notify
