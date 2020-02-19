# Big Data Pipeline For General Purpose Corpus Translation

This document is designed to understand the Big Data pipeline that is built for the multiple use cases under Anuvaad project. 
This should be read in parallel with the code under `project-anuvaad/data-pipeline` repository. Together, these constitute what we consider to be a scalable and automated solution to writing amd scheduling ETL jobs using Apache Spark (PySpark) and Airflow. This project includes the following :

- Core ETL processing using PySpark
- The ML based workflow using Airflow
- Default configuration for the translation pipeline

## ETL Project Structure

The project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- config.yml
 |-- airflow-scripts/
 |   |-- anuvaad_airflow.py
 |-- spark-scripts/
 |   |-- anuvaad_tool_1_sentence_extractor.py/
 |   |-- anuvaad_tool_1_token_extractor.py/
 |-- dependencies/
 |   |-- ...
```

The workflow related scripts go under the 'airflow-scripts' directory.</br>
The core ETL scripts go under 'spark-scripts' and the default configs are available under 'configs' directory.</br>
Place the required dependencies under 'dependencies' directory. </br>

## Fitting Anuvaad to the BigData Ecosystem


![Import UI](/screenshots/anuvaad_bigdata_pipeline.png?raw=true)
#### Data Acquisition :
Data is ingested into the system using web-scraping or some other means. The data lands in any external server (that interacts outside the network). This is done for security purposes. The frequency of ingestion could be daily or hourly (depending on the needs). Currently based on the requirements, real time ingestion might not be required. This data is then loaded into HDFS/Cloud Storage.


#### Data pre-processing  :
Extract and pre-process data from HDFS to prepare the training data. This is done with the help of Tool 1 and Tool 2, which will be used for generation of tokens.


#### Data Transformation :
Data is then transformed into a format, which can be understood by the Model training system. This is achieved by the use of Tool 3, where in the parallel sentences are generated.


#### Model Training :
We will feed the transformed parallel sentences (General/Judicial) to the Model training system, and capture the output.


#### Running the Model :
Using the trained model, get inferences on the test dataset stored in HDFS/Cloud Storage. 


#### Model Evaluation :
Analyze the model performance, based on the predicted results on test data.
</br></br>

## Anuvaad Data Pipeline Considerations
#### Scalability :
With the ever increasing volume of data, the respective pipelines and their data stores will need to be able to massively autoscale to accommodate future load and velocity. This includes the ability to modularize and add components (via containers or VMs, which can be toggled on and off as necessary).

#### Data Distribution :
This allows many clients to have access to data and supports operations (create, delete, modify, read, write) on that data. Each data file may be partitioned into several parts called chunks. Each chunk may be stored on different nodes, facilitating the parallel execution of applications.

#### Parallel Processing :
This is needed for performing large computations by dividing the workload between more than one nodes/executors.

#### Performance :
This factor Involves
Cost Reduction for storage & processing of data.
Better turn around time for the translation.

#### Fault Tolerance :
System should continue to provide correct performance of its specified tasks in presence of failure. This is required at every level (Storage, Processing, etc)
Storage : fault tolerance is achieved through data replication (in turn increasing data availability).
Processing : fault tolerance is achieved through failover mechanism 

#### Visualization :
Though a convenient feature, Visualization needs to be an integral part of Anuvaad for (and not limited to) the following reasons :
Review the quality of the translated text visually.
Spotting the trends for failed tokens
</br></br>

## Tech Stack
The proposed tech stack for the Anuvaad pipeline for in-house processing is shown below :
![Import UI](/screenshots/anuvaad_tech_stack.png?raw=true)
This may slightly differ if adopted for Cloud based platform, like AWS, GCP, or Azure.

## Airflow - ML pipeline
Sample run of the Airflow workflow is shown below : 
![Import UI](/screenshots/anuvaad_airflow_workflow.png?raw=true)
