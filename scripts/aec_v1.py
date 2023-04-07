from builtins import range
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.mysql_hook import MySqlHook
import datetime
from airflow.hooks.base_hook import BaseHook


args = {
    'owner': 'Airflow',
    'start_date': days_ago(1)
}
dag = DAG(
    dag_id='AEC_V1',
    default_args=args,
    schedule_interval='0 * * * *',
    catchup = False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['aec_v1_sqoop']
)


conn = BaseHook.get_connection('aec_v1_sqoop')
user = conn.login
password = conn.password
jobs = ['AEC_charges','AEC_customer','AEC_inbound_details','AEC_inbound_headers','AEC_inbound_loads',
'AEC_inventory','AEC_invoice','AEC_items','AEC_locations','AEC_lot','AEC_outbound_details',
'AEC_outbound_headers','AEC_outbound_loads','AEC_pub_rates','AEC_rates','AEC_transactions']

a = []

for i in range(len(jobs)):
    a.append(BashOperator(
            task_id=str(jobs[i]),
            bash_command="kinit -kt /usr/local/airflow/dags/config/cdhp-all-admin.keytab cdhp-all-admin@LINEAGELOG.NET; sqoop job --exec {JOB} -- --username={USER} --password={PASS} ".format(JOB=jobs[i],USER=user,PASS=password) ,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag))
    if i not in [0]:
        a[i-1] >> a[i]


