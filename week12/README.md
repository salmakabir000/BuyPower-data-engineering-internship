\# Mini Orchestrator



This project is a simple Python-based workflow orchestrator that runs tasks in the correct dependency order. I used a DAG structure where each task can depend on another task. The runner uses topological sorting so tasks such as extract, transform, and load are executed in the correct order.



Task states are tracked as pending, running, success, failed, or skipped. If a task fails, its downstream tasks are skipped. Run history and task history are stored in SQLite so previous DAG runs can be checked later using the status and logs commands.



The project also includes a command-line interface with commands for listing DAGs, running a DAG, checking run history, and viewing task logs. I connected a simple crypto ETL workflow with extract, transform, and load tasks and tested it successfully through the orchestrator.



