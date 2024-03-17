from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

# Define the default arguments
default_args = {
    'owner': 'eric369',
    'start_date': datetime(2024, 3, 17),
    'email': 'minhdungnguyen2003@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG('ETL_toll_data',
          default_args=default_args,
          description='Apache Airflow Final Assignment',
          schedule='@daily',
         )


# Define the tasks
unzip_data = BashOperator(
    task_id='unzip_data',    
    bash_command='tar -xvzf /home/minh03/project/airflow/dags/finalassignment/tolldata.tgz -C /home/minh03/project/airflow/dags/finalassignment/',
    dag=dag)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d ',' -f1-4 /home/minh03/project/airflow/dags/finalassignment/vehicle-data.csv > /home/minh03/project/airflow/dags/finalassignment/csv_data.csv",
    dag=dag)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f5-8 /home/minh03/project/airflow/dags/finalassignment/tollplaza-data.tsv | tr '\t' ',' > /home/minh03/project/airflow/dags/finalassignment/tsv_data.csv",
    dag=dag)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=" awk '{print $10,$11}' /home/minh03/project/airflow/dags/finalassignment/payment-data.txt | tr ' ' ',' > /home/minh03/project/airflow/dags/finalassignment/fixed_width_data.csv",
    dag = dag)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste /home/minh03/project/airflow/dags/finalassignment/csv_data.csv /home/minh03/project/airflow/dags/finalassignment/tsv_data.csv /home/minh03/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/minh03/project/airflow/dags/finalassignment/extracted_data.csv",
    dag = dag)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command= "awk -F',' '{OFS=\",\"}{ $4=toupper($4) }1' /home/minh03/project/airflow/dags/finalassignment/extracted_data.csv > /home/minh03/project/airflow/dags/finalassignment/transformed_data.csv",
    dag = dag)


# Define the task dependencies
unzip_data >> extract_data_from_csv
unzip_data >> extract_data_from_tsv
unzip_data >> extract_data_from_fixed_width
extract_data_from_csv >> consolidate_data
extract_data_from_tsv >> consolidate_data
extract_data_from_fixed_width >> consolidate_data
consolidate_data >> transform_data



