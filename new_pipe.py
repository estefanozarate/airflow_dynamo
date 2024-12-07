from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_datos_complejo',
    default_args=default_args,
    description='Un DAG con 5 funciones que se ejecuta cada 3 horas',
    schedule_interval='0 */3 * * *',  # Cron que ejecuta cada 3 horas
    tags=['complejo', '3horas'],
)

@task(dag=dag)
def tarea1():
    print("Soy la Tarea 1")

@task(dag=dag)
def tarea2():
    print("Soy la Tarea 2")

@task(dag=dag)
def tarea3():
    print("Soy la Tarea 3")

@task(dag=dag)
def tarea4():
    print("Soy la Tarea 4")

@task(dag=dag)
def tarea5():
    print("Soy la Tarea 5")

@task(dag=dag)
def tarea6():
    print("Soy la Tarea 6")

@task(dag=dag)
def tarea7():
    print("Soy la Tarea 7")

@task(dag=dag)
def tarea8():
    print("Soy la Tarea 8")

@task(dag=dag)
def tarea9():
    print("Soy la Tarea 9")

t1 = tarea1()
t2 = tarea2()
t3 = tarea3()
t4 = tarea4()
t5 = tarea5()
t6 = tarea6()
t7 = tarea7()
t8 = tarea8()
t9 = tarea9()

t1 >> t4
t2 >> t6

t3 >> t5 >> t6

t6 >> [t7, t8] >> t9

