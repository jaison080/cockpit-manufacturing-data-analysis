# Cockpit Dashboard for Manufacturing Plant

# ================================

## Problem Statement

As a manufacturing plant for ABC corporation operating at Kerala, we are trying to implement a cockpit dashboard which collects data from various systems connected to the manufacturing plat to get a good visibility of what is going on in the plant on a day-to-day basis. As a start we want to address two use cases mentioned below :

<ul>
<li>Our MES(Manufacturing Execution System) generates lot of data in different formats and it needs to be collected, cleaned, processed and reports need to be generated from it. This is currently made available in AWS S3 buckets as daily data dumps and the can be consumed by the data processing applications.</li>

<li>We have IoT enabled temperature sensors placed in machineries to collect temperature values at regular intervals. This data is continuously streamed to a message bus (Kafka) in JSON format. This data also needs to be collected and made available for real-time reporting dashboard to continuously monitor real-time temperature variations of respective components.</li>
</ul>

## Solution

We are using Apache Spark to process data from S3 buckets and make it available for downstream applications. We are using Bokeh to visualize the data. We are using Kafka to stream data from IoT sensors and make it available for downstream applications.

## Apache Spark

Apache Spark is an open-source, distributed processing system used for big data workloads. It utilizes in-memory caching, and optimized query execution for fast analytic queries against data of any size. It provides development APIs in Java, Scala, Python and R, and supports code reuse across multiple workloads—batch processing, interactive queries, real-time analytics, machine learning, and graph processing.

## PySpark

PySpark is an interface for Apache Spark in Python. It not only allows you to write Spark applications using Python APIs, but also provides the PySpark shell for interactively analysing your data in a distributed environment. PySpark supports most of Spark’s features such as Spark SQL, DataFrame, Streaming, MLlib (Machine Learning) and Spark Core.

## Medallion Architecture

A medallion architecture is a data design pattern used to logically organise data in a lake house, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables). Medallion architectures are sometimes also referred to as "multi-hop" architectures.

Image from Doc

### Bronze Layer (Raw Data)

The Bronze layer is where we land all the data from external source systems. The table structures in this layer correspond to the source system table structures "as-is," along with any additional metadata columns that capture the load date/time, process ID, etc. The focus in this layer is quick Change Data Capture and the ability to provide an historical archive of source (cold storage), data lineage, auditability, reprocessing if needed without rereading the data from the source system.

### Silver Layer (Cleansed and Conformed Data)

In the Silver layer of the lakehouse, the data from the Bronze layer is matched, merged, conformed and cleansed ("just-enough") so that the Silver layer can provide an "Enterprise view" of all its key business entities, concepts and transactions. (e.g. master customers, stores, non-duplicated transactions and cross-reference tables).

The Silver layer brings the data from different sources into an Enterprise view and enables self-service analytics for ad-hoc reporting, advanced analytics and ML. It serves as a source for Departmental Analysts, Data Engineers and Data Scientists to further create projects and analysis to answer business problems via enterprise and departmental data projects in the Gold Layer.

In the lakehouse data engineering paradigm, typically the ELT methodology is followed vs. ETL - which means only minimal or "just-enough" transformations and data cleansing rules are applied while loading the Silver layer. Speed and agility to ingest and deliver the data in the data lake is prioritized, and a lot of project-specific complex transformations and business rules are applied while loading the data from the Silver to Gold layer. From a data modeling perspective, the Silver Layer has more 3rd-Normal Form like data models. Data Vault-like, write-performant data models can be used in this layer.

### Gold Layer (Curated Business-Level Tables)

Data in the Gold layer of the lakehouse is typically organized in consumption-ready "project-specific" databases. The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins. The final layer of data transformations and data quality rules are applied here. Final presentation layer of projects such as Customer Analytics, Product Quality Analytics, Inventory Analytics, Customer Segmentation, Product Recommendations, Marking/Sales Analytics etc. fit in this layer. We see a lot of Kimball style star schema-based data models or Inmon style Data marts fit in this Gold Layer of the lakehouse.

So you can see that the data is curated as it moves through the different layers of a lakehouse. In some cases, we also see that lot of Data Marts and EDWs from the traditional RDBMS technology stack are ingested into the lakehouse, so that for the first time Enterprises can do "pan-EDW" advanced analytics and ML - which was just not possible or too cost prohibitive to do on a traditional stack. (e.g. IoT/Manufacturing data is tied with Sales and Marketing data for defect analysis or health care genomics, EMR/HL7 clinical data markets are tied with financial claims data to create a Healthcare Data Lake for timely and improved patient care analytics.)

## Implementation

### Report 1 (Operations Management Report)

<ol>
<li>Spark session is created using the AWS Access Key and AWS Secret Key.</li>
<li>Two schemas, ```resultsSchema``` and ```workOrdersSchema``` are defined.</li>
<li>```PlansShiftWise.csv``` is loaded into dataframe ```plans_df```.  It is a bronze layer data. Set ```bronze_plans_df = plans_df```. Here the data is separated using delimiter ```|```.</li>
<li>Null columns are removed to make the dataframe ```silver_plans_df ```.</li>
<li>```Results.csv```  is loaded into dataframe ```result_df``` based on the schema ```resultsSchema```. It is a bronze layer data. Set ```bronze_result_df = result_df```. Here the data is separated using delimiter ```,```.</li>
<li>Null columns are removed to make the dataframe ````silver_result_df```.</li>
<li>```RoutingStages.parquet```  is loaded into dataframe ```routing_df``` . It is a bronze layer data.So ```bronze_routing_df = routing_df```. Null columns are removed to make the dataframe ```silver_routing_df```.</li>
<li>Create ```combined_df``` as  join of ```work_orders_df```, ```combined_df``` where  ```WorkOrderId == work_orders.id``` . Drop columns ```RoutingStageName```, ```BatchID```, ```id```.</li>
<li>Create  dataframe ```work_orders_df``` based on the schema ```workOrdersSchema``` and load the given data ```Workorders.csv```. Here the data is separated using delimiter ```\t```.</li>
<li>Drop columns ```EcnNo```, ```EcnQunatity```, ```EcnStatus```,```ProductRevision```,```PlannedStartDate```, ```PlannedEndDate```, ```BlockedDate```, ```BlockedBy```, ```CreationDate```, ```DysonPONumber```, ```CustomerSKUNumber```, ```Company```, ```Division```  from ```work_orderst_df```</li>
<li>Set ```combined_df``` as join of ```work_orders_df```, ```combined_df``` where ```WorkOrderId == work_orders_df.Id```. Filter by ```combined_df.Surface == 1```, and drop column ```Id```.</li>
<li>Create dataframe ```actual_df``` as ```combined_df``` grouped by the columns ```ItemId```, ```SubWorkCenter``` according to the count of ```BoardId``` along with last modified date, time as ```Hour``` and ```Date```.</li>
<li>Set ```combined_df``` as join of ```plans_df```, 
where ```(actual_df.ItemId == plans_df.ItemNo) &  (actual_df.SubWorkCenter == plans_df.Station) & 
(actual_df.Hour == F.hour(plans_df.Hour)) & 
 (actual_df.Date == F.date_format(plans_df.Date, "yyyy-MM-dd"))```,Drop ```actual_df.Hour``` and ```actual_df.Date```</li>
<li>Create  dataframe ```items_df``` and load the given data ```Items.txt```. It is a bronze layer data. Set ```bronze_items_df = items_df```. </li>
<li>Create pattern which is a regex expression is a sequence of different characters which describe the particular search pattern. </li>
<li>Create schema called ```itemsSchema``` as given.</li>
<li>Define function ```extract_values``` to extract data from input dataframe with the created pattern.</li>
<li>Create dataframe ```extract_values_udf```  and extract values into it according to the created ```itemsSchema```,</li>
<li>In ```items_df``` create column ```structured_data``` and load value from ```extract_values_udf```.</li>
<li>Create columns in ```items_df``` as,
```items_df = items_df.withColumn("ID_Items", F.col("structured_data.ID"))```</li>
<li>Continue this operation for all columns.</li>
<li>From ```items_df``` drop column ```structured_data```.</li>
<li>Now items_df is a silver layer data, Create ```silver_items_df = items_df```</li>
<li>Remove the rows with null values by ```items_df=items_df.na.drop()```</li>
<li>Create ```final_df``` as join of ```items_df```, ```combined_df``` where ```ItemId``` == ```items_df.ID_Items```. This is gold layer data. Set ```gold_df = final_df```.</li>
<li>Create grouped_data as ```final_df.groupBy(“Hour”, “Date”, “ItemNo”)```</li> 
<li>Create ```actual_production``` which is the sum of column ```ActualQuantity```</li>
<li>Create ```planned_production``` which is the sum of column ```Quantity```.</li>
 
 
<li>Planned production vs actual production is shown by dataframe which shows ```actual_production```, ```planned_production```, ```Hour```, ```Date```, ```ItemNo```, and a new column called ```Difference``` which is ,
```F.col("Planned Production") - F.col("Actual Production")```.
</li>
</ol>
