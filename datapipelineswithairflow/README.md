# Sparkify Load Automation and Monitoring 

## Summary

This is the Sparkify Data Warehouse built in the Cloud using Amazon AWS Redshift database. The ETL is automated using high grade data pipelines using Apache Airflow.


### ETL Pipeline Description  

The pipeline is implemented using Apache Airflow. It consists of a DAG (Directed Acyclic Graph) that loads data using reusable tasks that make use of dynamic parameters.

#### udac_example_dag
The main class defining the DAG is called udac_example_dag. This calls various mostly reusable operators to load staging tables, fact and dimension tables
There is also an operator to perform data quality checks

#### StageToRedshiftOperator
This operator implements a reusable task to load json files from Amazon S3 into Amazon Redshift staging tables. It also implements a templated key that provides
the ability to load staging data based on the load frequency


#### LoadFactOperator
This operator implements code to load the songplays fact table from staging tables loaded by the previous operator

#### LoadDimensionOperator
As the name implies, this operator implements a reusable task to load dimension tables based on the input insert query parameter passed. It also has a parameter
to indicate if the tables have to be truncated before loading

#### DataQualityOperator
This operator runs the data quality check SQL passed as a parameter and compares the output of the query to the expected output parameter. If the results do
not match the pipeline fails indicating data quality issues

### 
### Runtime instructions

Once the Airflow process is started, the DAG can be kicked off in the Airflow GUI and the pipeline can be managed from the GUI. Runtime logs can also be viewed
in the Airflow GUI. The pipeline can also be monitored as it runs through various stages. 
