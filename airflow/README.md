### Extraction, Transformation and Loading (ETL) pipeline using Airflow
Welcome to the introduction of this repositry. This repositry will cover how to setup and run and etl pipeline using airflow. It is important to make sure that all steps are followed closely and pre-required tools/ packages are installed and working. This will be a very simple etl pipeline which extracts data from a csv file taken from Kaggle, transforms the data and then loads it into MsSQL. Please feel free to reach out for any questions regarding my work!
### Airflow
Apache airflow is a platform desgined to orchestracte, schedule and monitor workflows. This is an ideal platform for ETL pipelines due to its DAG (Directed Acyclic Graph) arhitecture and easy use of task allocation.
### Docker
Docker is a platform which allows you to run containerise operations almost like virtual machines (VMs). It allows you to build test and deploy applications quickly whilst keeping all reducing dependancy conflicts due to applications being run in containers. This works great with airflow for the previously mentioned reason.

### Data
 **Retial Sales Dataset - Kaggle <br>**
 **Link -** https://www.kaggle.com/datasets/mohammadtalib786/retail-sales-dataset <br>
 Welcome to the Retail Sales and Customer Demographics Dataset! This synthetic dataset has been meticulously crafted to simulate a dynamic retail environment, providing an ideal playground for those eager to sharpen their data analysis skills through exploratory data analysis (EDA). With a focus on retail sales and customer characteristics, this dataset invites you to unravel intricate patterns, draw insights, and gain a deeper understanding of customer behavior.
### Extraction, Transformation and Loading stages
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
### Instructions
1. 
1. Download 'docker-compose.yaml' and 'requirements.txt' file into your working directory
#### 2. Download 'dags' and 'data' folders into your working directory
#### 3. In your working directory run:
```
python -m venv .venv
```
and then:
```
.venv\Scripts\activate
```
#### 4. Run requiremnets.txt
```
pip install -r requirements.txt
```
#### 5. Open docker desktop (make sure no airflow containers are running)
![](images/containers_not_running.PNG)
#### 6. Initialise airflow in docker:
```
docker-compose up airflow-init
```
#### 7. Launch Airflow
```
docker-compose up
```
#### 8. Make sure all images are running
![](images/containers_running.PNG)
#### 9. Open airflow through the port link in the api-server image
![](images/port.PNG)
#### 10. Trigger the etl dag
![](images/trigger.PNG)
#### 11. Results
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