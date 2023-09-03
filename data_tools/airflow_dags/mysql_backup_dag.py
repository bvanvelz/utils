from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import datetime
import os
import yaml

# Define your DAG
dag = DAG(
    'mysql_backup',
    description='Backup MySQL database daily',
    schedule_interval='@daily',  # Run the DAG daily
    start_date=days_ago(1),  # Start one day ago to run immediately
    catchup=False,  # Disable catchup
    default_args={
        'owner': 'your_name',
        'depends_on_past': False,
        'retries': 1,
    },
)

# Define a Python function to perform the backup
def mysql_backup(config_path):
    # Load configuration from the YAML file
    with open('./config/mysql_backup.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)

    today = datetime.datetime.now()
    year = today.year
    month = today.month
    day = today.day
    backup_folder = f'{config["backup_folder"]}/{year}/{month:02d}'
    backup_filename = f'{day:02d}.sql'
    os.makedirs(backup_folder, exist_ok=True)
    backup_path = os.path.join(backup_folder, backup_filename)

    # Replace with your MySQL backup command (e.g., using mysqldump)
    mysql_dump_command = f'mysqldump -u {config["username"]} -p{config["password"]} {config["database"]} > {backup_path}'
    os.system(mysql_dump_command)

# PythonOperator to run the backup function
backup_task = PythonOperator(
    task_id='mysql_backup_task',
    python_callable=mysql_backup,
    op_args=['/path/to/config.yaml'],  # Specify the path to your YAML config file
    dag=dag,
)

# You can add additional tasks or dependencies as needed

if __name__ == "__main__":
    dag.cli()
