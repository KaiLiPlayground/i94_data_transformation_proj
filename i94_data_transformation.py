import airflowlib.emr_lib as emr
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
dag = DAG('i94_data_transformation', default_args=default_args, schedule_interval=None, concurrency=3)

# Define the task functions
def submit_spark_job(task_script):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'spark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    script_path = f'/root/airflow/dags/I94analytics/{task_script}'
    statement_response = emr.submit_statement(session_url, script_path)
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)


'''
Dynamic Task Creation
Task Names Array: task_names is an array containing the names of the tasks that need to be created.

Looping Over Task Names: The for loop iterates over each name in the task_names array.

PythonOperator Creation: Inside the loop, a PythonOperator is created for each task. The task_id is set to the name of the task, and the python_callable is set to submit_spark_job, which is a function that handles the execution of the task.

op_kwargs Parameter: The op_kwargs argument is used to pass the name of the Python script file that corresponds to each task. This script name is constructed by appending .py to the task name (e.g., normalize_dag_check.py).

Storing in globals(): globals()[task_name] assigns the created PythonOperator to a global variable with the same name as the task. This allows each task to be referenced by its name later in the DAG.

Setting Dependencies
Sequential Execution: The dependencies are defined using the >> operator, which sets up a sequence of tasks to be executed in a specific order.
Flow of Execution:
create_cluster starts first. Once it's completed, wait_for_cluster_completion begins.
After wait_for_cluster_completion, normalize_dag_check is triggered.
This sequential triggering continues down to quality_check.
Finally, after quality_check completes, terminate_cluster is executed.
'''

# Define PythonOperators for each task
task_names = ["normalize_dag_check", "transform_immigration", "transform_immigration_demographics", "quality_check"]
for task_name in task_names:
    globals()[task_name] = PythonOperator(
        task_id=task_name,
        python_callable=submit_spark_job,
        op_kwargs={'task_script': f'{task_name}.py'},
        dag=dag
    )

# Existing definitions
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

# Set the dependencies as per the new structure
create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> normalize_dag_check
normalize_dag_check >> transform_immigration
transform_immigration >> transform_immigration_demographics
transform_immigration_demographics >> quality_check
quality_check >> terminate_cluster
