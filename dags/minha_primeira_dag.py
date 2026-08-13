from airflow.decorators import dag, task
from datetime import datetime
from time import sleep


@dag(
    dag_id="minha_primeira_dag",
    description="Exemplo de DAG simples com Airflow",
    schedule="* * * * *",  # Executa a cada minuto
    start_date=datetime(2026, 1, 1),
    catchup=False,  # Evitar backfill de execuções passadas
)
def minha_primeira_pipeline():
    @task
    def primeira_atividade():
        print("Executando a primeira atividade da DAG!")
        sleep(5)  # Simula uma tarefa que leva 5 segundos para ser concluída

    @task
    def segunda_atividade():
        print("Executando a segunda atividade da DAG!")
        sleep(3)  # Simula uma tarefa que leva 3 segundos para ser concluída

    @task
    def terceira_atividade():
        print("Executando a terceira atividade da DAG!")
        sleep(2)  # Simula uma tarefa que leva 2 segundos para ser concluída

    @task
    def quarta_atividade():
        print("Executando a quarta atividade da DAG!")
        sleep(1)  # Simula uma tarefa que leva 1 segundo para ser concluída

    t1 = primeira_atividade()
    t2 = segunda_atividade()
    t3 = terceira_atividade()
    t4 = quarta_atividade()

    (
        t1 >> t2 >> t3 >> t4
    )  # Define a ordem de execução: primeira_atividade -> segunda_atividade -> terceira_atividade -> quarta_atividade

    # t1 >> [t2, t3] # Define que a segunda_atividade e a terceira_atividade dependem da primeira_atividade
    # t3 << t4  # Define que a quarta_atividade depende da terceira_atividade

    # t1.set_downstream([t2,t3]) # downstream indica que a t2 e t3 dependem da execução da t1
    # t3.set_upstream(t4) # upstream indica que a t3 libera a execução da t4, ou seja, a t4 só será executada após a t3 ser concluída

    # chain(t1, t2, t3, t4)  # define a ordem de execução das tarefas


minha_primeira_pipeline()  # instancia a DAG para que o Airflow possa reconhecê-la
