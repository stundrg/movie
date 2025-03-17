from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.dag import DAG

from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)

with DAG(
    "task",
    schedule="@hourly",
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    branch.op = EmptyOperator(task_id="branch")
    rm.dir = EmptyOperator(task_id="rm_dir")
    echo.task = EmptyOperator(task_id="echo_task")
    get.start = EmptyOperator(task_id="get_start")
    no.param = EmptyOperator(task_id="no_param")
    multi.n = EmptyOperator(task_id="multi_n") 
    multi.y = EmptyOperator(task_id="multi_y")
    nation.f = EmptyOperator(task_id="nation_f")
    nation.k = EmptyOperator(task_id="nation_k")
    get.end = EmptyOperator(task_id="get_end")
    merge.data = EmptyOperator(task_id="merge")

start >> branch.op
branch.op >> [rm.dir,echo.task]
[rm.dir,echo.task] >> get.start
branch.op >> get.start
get.start >> [no.param,multi.n,multi.y,nation.f,nation.k]
[no.param,multi.n,multi.y,nation.f,nation.k] >> get.end
get.end >> merge.data
merge.data >> end
