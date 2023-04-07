

from airflow import DAG
from airflow.utils.helpers import chain
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import configparser
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from utils.send_notification import send_slack_notification

args = {
    'owner': 'Airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'on_failure_callback':send_slack_notification,
    'email_on_failure':True,
    'email': 
    'retry_delay': timedelta(minutes=2)
}

config = configparser.ConfigParser()
config.read('/usr/local/airflow/dags/config/accellos_processed_conf.cfg')
for app in config.sections():
    # Get the Source Connection Details:
    #schedule = config[app]['schedule']
    schedule = None if config[app]['schedule'] == 'None' else config[app]['schedule']
    parent_dir = config[app]['parent_dir']
    batch_file = config[app]['raw_batch_file']
    tag_id = config[app]['tag_id']
    dag_id = config[app]['dag_id']
    keytab_user = config[app]['keytab_user']
    keytab_file = config[app]['keytab_file']
    trigger_dag_seq = config[app]['processed_with_seq_dag_id']
    trigger_dag_non_seq = config[app]['processed_without_seq_dag_id']

    # Task 01: INITILIZE THE DAG FOR THE APPLICATION
    dag=globals()[dag_id] = DAG(dag_id, schedule_interval=schedule, default_args=args, catchup=False, max_active_runs=1,tags=[tag_id])

    with dag:
        table_file = open(batch_file).read().splitlines()
        result = []
        for i in table_file:
            tmp = i.split(",")
            result.append((int(tmp[0]), tmp[1]))


        from collections import defaultdict
        d = defaultdict()

        for k, *v in result:
            d.setdefault(k, []).append(v)

        tables_list = list(d.items())
        # Task 02: START OF THE DAG USING DUMMY OPERATOR
        start = DummyOperator(task_id='Start', dag=dag)

        # Task 03: END OF THE DAG USING DUMMY OPERATOR
        end = DummyOperator(task_id='End', dag=dag)

        trigger_processed = DummyOperator(task_id='Trigger_Processed_Task', trigger_rule='all_done', dag=dag)

        trigger_task_1 = TriggerDagRunOperator(
                dag=dag,
                task_id=f'Trigger_{trigger_dag_seq}',
                trigger_dag_id=f'{trigger_dag_seq}',trigger_rule='all_done',
                retries=3
            )

        trigger_task_2 = TriggerDagRunOperator(
                dag=dag,
                task_id=f'Trigger_{trigger_dag_non_seq}',
                trigger_dag_id=f'{trigger_dag_non_seq}',trigger_rule='all_done',
                retries=3
            )

        tasks_lists_actv=[]
        tasks_lists_del=[]
        for i in tables_list:
            batchid = i[0]
            table_flat_list = i[1]
            flat_list = [item for sublist in table_flat_list for item in sublist]
            raw_active_lists=[]
            raw_delete_lists=[]
            raw_list=[]
            sqp_lists=[]
            alter_lists=[]
            #Task 04: START OF THE BATCH
            start_batch = BashOperator(task_id=f'Start_Batch_{batchid}',
                                                 bash_command='kinit -kt /usr/local/airflow/dags/utils/{}  {} '.format(keytab_file,keytab_user), dag=dag, trigger_rule='all_done')
            tasks_lists_actv.append(start_batch)
            tasks_lists_del.append(start_batch)

            for table in flat_list:
                # Task 05: TO IMPORT DATA FROM SOURCE TO STAGE LAYER USING SQOOP
                sqoop_import_task = BashOperator(task_id=f'SourceToStage_{table}',
                                                 bash_command='{}/stage/{}.sh '.format(parent_dir,table), dag=dag)

                # Task 06: TO LOAD DATA FROM STAGE TO RAW ACTIVE LAYER USING HIVE
                raw_active_load_task = BashOperator(task_id=f'StageToRaw_{table}',
                                                 bash_command='python3 {}/raw/upsert/{}.py '.format(parent_dir,table), dag=dag)

                # Task 07: TO LOAD DATA FROM STAGE TO RAW DELETE LAYER USING HIVE
                raw_delete_load_task = BashOperator(task_id=f'StageToRaw_{table}_del',
                                                 bash_command='python3 {}/raw/delete/{}.py '.format(parent_dir,table), dag=dag)

                # Task 08: TO UPDATE TABLE LOCATION FROM STAGE CURRENT TO STAGE PREVIOUS USING HIVE
                alter_task = BashOperator(task_id=f'StageCurrToPrevSwitch_{table}',
                                                 bash_command='python3 {}/raw/alter_script/{}.py '.format(parent_dir,table), dag=dag)
                alter_lists.append(alter_task)
                sqp_lists.append(sqoop_import_task)
                raw_active_lists.append(raw_active_load_task)
                raw_delete_lists.append(raw_delete_load_task)

            tasks_lists_actv.append(sqp_lists)
            tasks_lists_actv.append(raw_active_lists)
            tasks_lists_del.append(sqp_lists)
            tasks_lists_del.append(raw_delete_lists)
            tasks_lists_actv.append(alter_lists)
            tasks_lists_del.append(alter_lists)
        chain(start, *tasks_lists_actv, trigger_processed, trigger_task_1, end)
        chain(start, *tasks_lists_del, trigger_processed, trigger_task_2, end)