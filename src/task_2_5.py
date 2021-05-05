"""
- - - - - - - - - - - - - - - - - - - - - -
 TRUATA DE CODING CHALLENGE V1
- - - - - - - - - - - - - - - - - - - - - -
 Candidate: Weverton de Souza Castanho
 Email: wevertonsc@gmail.com
 Data: 28.APRIL.2021
- - - - - - - - - - - - - - - - - - - - - -
"""
from airflow import DAG
from airflow.exceptions import AirflowDagCycleException
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dag_cycle_tester import test_cycle
from tests.models import DEFAULT_DATE


def truata_teste_cycle_no_cycle(self):
    # Truata Part 2 Task 5
    dag = DAG(
        'dag',
        start_date=DEFAULT_DATE,
        default_args={'owner': 'owner1'})

    # Task_1 -> Task_2 -> Task_4
    #        -> Task_3 -> Task_5
    #                  -> Task_6

    with dag:
        op1 = DummyOperator(task_id='Task_1')
        op2 = DummyOperator(task_id='Task_2')
        op3 = DummyOperator(task_id='Task_3')
        op4 = DummyOperator(task_id='Task_4')
        op5 = DummyOperator(task_id='Task_5')
        op6 = DummyOperator(task_id='Task5')
        op1.set_downstream(op2)
        op1.set_downstream(op3)
        op2.set_downstream(op4)
        op3.set_downstream(op5)
        op3.set_downstream(op6)

    self.assertFalse(test_cycle(dag))
