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
         #dagrun_timeout=timedelta(minutes=2),
         schedule_interval='30 0 * * *', catchup = False) as dag:

      ETL_VALIDA_INTERVALO_OS = CarteTransOperator(
        dag=dag,
        task_id='ETL_VALIDA_INTERVALO_OS',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_VALIDA_INTERVALO_OS',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DECA_RESUMIDA = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DECA_RESUMIDA',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_DECA_RESUMIDA',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_SOCIOS = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_SOCIOS',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_SOCIOS',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_CODOCOR = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_CODOCOR',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_CODOCOR',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DADOS_CONTRIBUINTE_PRE = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DADOS_CONTRIBUINTE_PRE',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_DADOS_CONTRIBUINTE_PRE',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DADOS_CONTRIBUINTE = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DADOS_CONTRIBUINTE',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_DADOS_CONTRIBUINTE',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DADOS_SOCIOS = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DADOS_SOCIOS',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_DADOS_SOCIOS',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DADOS_AUTUACOES = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DADOS_AUTUACOES',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_DADOS_AUTUACOES',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DADOS_AIDFS = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DADOS_AIDFS',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_DADOS_AIDFS',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DADOS_DAES = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DADOS_DAES',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_DADOS_DAES',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DADOS_GIAST = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DADOS_GIAST',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_DADOS_GIAST',
        params={'date': '{{ ds }}'}
      )

      ETL_CADASTRO_DADOS_DAPIRES = CarteTransOperator(
        dag=dag,
        task_id='ETL_CADASTRO_DADOS_DAPIRES',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_CADASTRO_DADOS_DAPIRES',
        params={'date': '{{ ds }}'}
      )

      ETL_LIMPA_ATRIBUTOS_TAGS = CarteTransOperator(
        dag=dag,
        task_id='ETL_LIMPA_ATRIBUTOS_TAGS',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_LIMPA_ATRIBUTOS_TAGS',
        params={'date': '{{ ds }}'}
      )

      JOB_LIMPA_ARQUIVOS = CarteJobOperator(
        dag=dag,
        task_id="JOB_LIMPA_ARQUIVOS",
        job="/ETL_AUDITOR_ELETRONICO/JOB_LIMPA_ARQUIVOS",
        params={"date": "{{ ds }}"})

      ETL_GERACAO_ARQUIVOS_LOOP = CarteTransOperator(
        dag=dag,
        task_id='ETL_GERACAO_ARQUIVOS_LOOP',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_GERACAO_ARQUIVOS_LOOP',
        params={'date': '{{ ds }}'}
      )

      JOB_CRIACAO_ARQUIVOS = CarteJobOperator(
        dag=dag,
        task_id="JOB_CRIACAO_ARQUIVOS",
        job="/ETL_AUDITOR_ELETRONICO/JOB_CRIACAO_ARQUIVOS",
        params={"date": "{{ ds }}"})

      JOB_VERIFICA_MONTAGEM = CarteJobOperator(
        dag=dag,
        task_id="JOB_VERIFICA_MONTAGEM",
        job="/ETL_AUDITOR_ELETRONICO/JOB_VERIFICA_MONTAGEM",
        params={"date": "{{ ds }}"})

      ETL_COPIAR_ARQUIVOS_LOOP = CarteTransOperator(
        dag=dag,
        task_id='ETL_COPIAR_ARQUIVOS_LOOP',
        trans='/ETL_AUDITOR_ELETRONICO/ETL_COPIAR_ARQUIVOS_LOOP',
        params={'date': '{{ ds }}'}
      )

      JOB_MOVER_ARQUIVOS_PASTA_USUARIO = CarteJobOperator(
        dag=dag,
        task_id="JOB_MOVER_ARQUIVOS_PASTA_USUARIO",
        job="/ETL_AUDITOR_ELETRONICO/JOB_MOVER_ARQUIVOS_PASTA_USUARIO",
        params={"date": "{{ ds }}"})

      JOB_ENVIAR_EMAIL_FINAL = CarteJobOperator(
        dag=dag,
        task_id="JOB_ENVIAR_EMAIL_FINAL",
        job="/ETL_AUDITOR_ELETRONICO/JOB_ENVIAR_EMAIL_FINAL",
        params={"date": "{{ ds }}"})

      JOB_FINALIZA_CARGA = CarteJobOperator(
        dag=dag,
        task_id="JOB_FINALIZA_CARGA",
        job="/ETL_AUDITOR_ELETRONICO/JOB_FINALIZA_CARGA",
        params={"date": "{{ ds }}"})
      
      ETL_VALIDA_INTERVALO_OS >> ETL_CADASTRO_DECA_RESUMIDA
      ETL_VALIDA_INTERVALO_OS >> ETL_CADASTRO_SOCIOS
      ETL_VALIDA_INTERVALO_OS >> ETL_CADASTRO_CODOCOR
      ETL_VALIDA_INTERVALO_OS >> ETL_CADASTRO_DADOS_CONTRIBUINTE_PRE  
      ETL_CADASTRO_DADOS_CONTRIBUINTE_PRE >> ETL_CADASTRO_DADOS_CONTRIBUINTE 
      ETL_CADASTRO_DADOS_CONTRIBUINTE >> ETL_CADASTRO_DADOS_SOCIOS
      ETL_CADASTRO_DADOS_SOCIOS >> ETL_CADASTRO_DADOS_AUTUACOES
      ETL_CADASTRO_DADOS_AUTUACOES >> ETL_CADASTRO_DADOS_AIDFS
      ETL_CADASTRO_DADOS_AIDFS >> ETL_CADASTRO_DADOS_DAES
      ETL_CADASTRO_DADOS_DAES >> ETL_CADASTRO_DADOS_GIAST
      ETL_CADASTRO_DADOS_GIAST >> ETL_CADASTRO_DADOS_DAPIRES
      ETL_CADASTRO_DADOS_DAPIRES >> ETL_LIMPA_ATRIBUTOS_TAGS
      ETL_VALIDA_INTERVALO_OS >> JOB_LIMPA_ARQUIVOS >> ETL_GERACAO_ARQUIVOS_LOOP >> JOB_CRIACAO_ARQUIVOS >> JOB_VERIFICA_MONTAGEM >> ETL_COPIAR_ARQUIVOS_LOOP >> JOB_MOVER_ARQUIVOS_PASTA_USUARIO >> JOB_ENVIAR_EMAIL_FINAL >> JOB_FINALIZA_CARGA      
