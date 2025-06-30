# airflow_example



ETL -> Extract Transform Load

AS we Extract data from different sources and data changes hence we need to schedule it daily, weekly or montly basis

To schedule the ETL workflows, we use AirFLow. Apache airflow is an open source, used to run pipelines -> ETL, datascience. 

We use Astromer.io to run Airflow


winget install -e --id Astronomer.Astro  - Install Astromer.io needs docker to be installed in your system.

astro dev init


Airflow to create our workflows 

We use DAG -> Directed Acyclic Graph

Every step in the workflow we define, we have to consider this as one task.

Task 1:- Pulling data from api

Task 2:- Transformation

Task 3:- Load the data into eg postgres

What is directed acyclic graph. Have to execute the task in sequence, DAg means there is some direction. Acyclic means we don't have any self loop.

Nodes(Tasks) -Direction> Nodes(Tasks)


inorder to run container locally we need to create docker-compose.yml file.

astro dev start
1. will run entire airflow package in a container and then along with that because my docker compose has another container to run for postgres will also run that.

astro dev restart

After making any changes

make connections in airflow 

then astro dev restart

to connect postgres database use dbeaver 