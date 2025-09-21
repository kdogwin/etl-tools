### ETL pipeline using Airflow (CSV into MsSQL)
Welcome to the introduction of this repositry. This repositry will cover how to setup and run and etl pipeline using airflow. It is important to make sure that all steps are followed closely and pre-required tools/ packages are installed and working. This will be a very simple etl pipeline which extracts data from a csv file taken from Kaggle, transforms the data and then loads it into MsSQL. Please feel free to reach out for any questions regarding my work!
### Airflow
Apache airflow is a platform desgined to orchestracte, schedule and monitor workflows. This is an ideal platform for ETL pipelines due to its DAG (Directed Acyclic Graph) arhitecture and easy use of task allocation.
### Docker
Docker is a platform which allows you to run containerise operations almost like virtual machines (VMs). It allows you to build test and deploy applications quickly whilst keeping all reducing dependancy conflicts due to applications being run in containers. This works great with airflow for the previously mentioned reason.

### Data
 **Retial Sales Dataset - Kaggle <br>**
 **Link -** https://www.kaggle.com/datasets/mohammadtalib786/retail-sales-dataset <br>
 Welcome to the Retail Sales and Customer Demographics Dataset! This synthetic dataset has been meticulously crafted to simulate a dynamic retail environment, providing an ideal playground for those eager to sharpen their data analysis skills through exploratory data analysis (EDA). With a focus on retail sales and customer characteristics, this dataset invites you to unravel intricate patterns, draw insights, and gain a deeper understanding of customer behavior.
### ETL stages
- **Extraction** - Hard copy data. This is in the form of a csv, as allocated in the data folder. During the extraction process the data is pulled from the provided path and insterted into a Pandas DataFrame (in memory). This is then returned back to the function to pass onto another function.
- **Transformation** - Data is retreived from the extraction phase and transformed using cleaning techniques and segmenting data into desingated tables like customer, products etc. This is also returned.
- **Loading** - Finally the data is converted into a list of dictionaries so SQLAlchemy can map the object into sql. This is the final part of the process and it is important to make sure that all relevant data has fully loaded in.
### Pre-required tools
- Docker Desktop
- IDE (Python executable)
- IDE (SQL executable)
- Python 3.8
- ODBC Driver for SQL server (17 or 18)
- Access to SQL server (Docker, SQLServerManagment, VisualStudio, etc)
### Installations
1. Unzip airflow folder
2. Run airflow_38_venv scripts to create python virtual environment:
```
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypassv
.\airflow_38_venv\Scripts\Activate.ps1
```
your venv should now be activated by showing:
```
(airflow_38_venv) - path/to/airflow
```
2. Install requirements.txt:
```
pip install -r requirements.txt
```
3. Open docker desktop (make sure no airflow or postgres containers are running)
4. Initialise airflow:
```
docker-compose up airflow-init
```
5. Launch Airflow
```
docker-compose up
```
6. Container and images should be successfully created (repo will be airflow)

![](images/containers_running.PNG)
### Configurations
1. Sign in to airflow GUI (may need to wait until connection is established)
- Username: airflow 
- Password: airflow

![](images/port.PNG)
2. Connect your odbc
- Home -> Admin -> Connections -> Add Connection.
- Enter the following information and make changes to "dsn" to match your MsSQL setup
- Save connection

![](images/odbc_conn.PNG)
### Running
1. Run 'retail_sales.sql' in folder 'sql' to create the database.
- Make sure to use the same server that was assigned in airflow.
- Check the database has been created by running (in SQL):
```
USE retial_sales;

select * from customer
```
- You should see the customer table but not data inserted
2. Enable the 'retial_sales_etl' dag back in airflow GUI: Home -> Dags
- Make sure it is turned on

![](images/trigger.PNG)
3. Open and trigger the dag!
4. The ETL pipeline should run successfully:
- Data will be inserted into your MsSQL server!

![](images/pipeline.PNG)

### Contribution guidelines for future developers.
### Mapping
pandas.to_sql is ideal for loading small amounts of data (few thousand rows) into SQL server.
**SQLAlchemy** - Alchemy is a python library used for interacting with sql databases. There are two main components:
- Core - lets you write sql statements in a pythonic way
- Object relation mapping - maps python classes/dfs to db tables
**Engine** - Is the starting point for and SQLAlchemy Application. It:
- manages connections to the database
- never connect directly to the db, the engine handles connections
**Connection String** - provides the required variables for the engine to connect to the db
```
dialect+driver://username:password@host:port/database?options 
                                |
                                V
mssql+pyodbc://@DESKTOP-1TNQNLI\\SQLEXPRESS/retail_sales""?driver=ODBC+Driver+18+for+SQL+Server""&Trusted_Connection=yes
```
**Driver** - driver is used to translate queries to the SQL Server (Tabular Data Stream - SQL server protocol used for communication)
- Python speaks to Driver, driver speaks to TDS
### More info
Contact me for issues with running the script
### Contributions
kdogwin44