from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow_pentaho.operators.kettle import KitchenOperator
from airflow_pentaho.operators.kettle import PanOperator
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

DAG_NAME = 'DAG_AEBR_Loop'
DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         dagrun_timeout=timedelta(minutes=2),
         schedule_interval='30 0 * * *', catchup = False) as dag:

      ETL_VALIDA_INTERVALO_OS = CarteTransOperator(
        dag=dag,
        task_id='ETL_VALIDA_INTERVALO_OS',
        trans='ETL_AUDITOR_ELETRONICO\ETL_VALIDA_INTERVALO_OS',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DECA_RESUMIDA = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DECA_RESUMIDA',
        trans='ETL_AUDITOR_ELETRONICO\ETL_CADASTRO_DECA_RESUMIDA',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_SOCIOS = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_SOCIOS',
        trans='ETL_AUDITOR_ELETRONICO\ETL_CADASTRO_SOCIOS',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_CODOCOR = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_CODOCOR',
        trans='ETL_AUDITOR_ELETRONICO\ETL_CADASTRO_CODOCOR',
        params={'date': '{{ ds }}'}
      )
    
      ETL_VALIDA_INTERVALO_OS >> ETL_CADASTRO_DECA_RESUMIDA
      ETL_VALIDA_INTERVALO_OS >> ETL_CADASTRO_SOCIOS
      ETL_VALIDA_INTERVALO_OS >> ETL_CADASTRO_CODOCOR
        
