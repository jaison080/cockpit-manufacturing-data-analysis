# Cockpit Dashboard for Manufacturing Plant

### Team Name : CODE MINERS

### Team Members :

1. [Jaison Dennis](https://github.com/jaison080)
2. [Jagannath E Shahi](https://github.com/Jagannathes)
3. [Hana Shelbin](https://github.com/H-ana)
4. [Rahul VS](https://github.com/rahulvs891)

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Solution](#solution)
3. [Apache Spark](#apache-spark)
4. [PySpark](#pyspark)
5. [Medallion Architecture](#medallion-architecture)
6. [Implementation](#implementation)
7. [Report 1 (Operations Management Report)](#report-1-operations-management-report)
8. [Report 2 (SC Supply Chain Report)](#report-2-sc-supply-chain-report)
9. [Report 3 (Component Temperature Realtime Report)](#report-3-component-temperature-realtime-report)
10. [Unit Test Cases](#unit-test-cases)
11. [Scalability](#scalability)
12. [Deployment](#deployment)
13. [Handling Exceptions](#handling-exceptions)
14. [Conclusion](#conclusion)
15. [References](#references)

## Problem Statement

As a manufacturing plant for ABC corporation operating at Kerala, we are trying to implement a cockpit dashboard which collects data from various systems connected to the manufacturing plat to get a good visibility of what is going on in the plant on a day-to-day basis. As a start we want to address two use cases mentioned below :

<ul>
<li>Our MES(Manufacturing Execution System) generates lot of data in different formats and it needs to be collected, cleaned, processed and reports need to be generated from it. This is currently made available in AWS S3 buckets as daily data dumps and the can be consumed by the data processing applications.</li>

<li>We have IoT enabled temperature sensors placed in machineries to collect temperature values at regular intervals. This data is continuously streamed to a message bus (Kafka) in JSON format. This data also needs to be collected and made available for real-time reporting dashboard to continuously monitor real-time temperature variations of respective components.</li>
</ul>

Go to [Table of Contents](#table-of-contents)

## Solution

We are using Apache Spark to process data from S3 buckets and make it available for downstream applications. We are using Bokeh to visualize the data. We are using Kafka to stream data from IoT sensors and make it available for downstream applications.

Go to [Table of Contents](#table-of-contents)

## Apache Spark

Apache Spark is an open-source, distributed processing system used for big data workloads. It utilizes in-memory caching, and optimized query execution for fast analytic queries against data of any size. It provides development APIs in Java, Scala, Python and R, and supports code reuse across multiple workloads—batch processing, interactive queries, real-time analytics, machine learning, and graph processing.

Go to [Table of Contents](#table-of-contents)

## PySpark

PySpark is an interface for Apache Spark in Python. It not only allows you to write Spark applications using Python APIs, but also provides the PySpark shell for interactively analysing your data in a distributed environment. PySpark supports most of Spark’s features such as Spark SQL, DataFrame, Streaming, MLlib (Machine Learning) and Spark Core.

Go to [Table of Contents](#table-of-contents)

## Medallion Architecture

A medallion architecture is a data design pattern used to logically organise data in a lake house, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables). Medallion architectures are sometimes also referred to as "multi-hop" architectures.

![image](https://user-images.githubusercontent.com/93505829/212487243-420dbda5-a85a-4cc1-ae01-119d1dadffd8.png)

### Bronze Layer (Raw Data)

The Bronze layer is where we land all the data from external source systems. The table structures in this layer correspond to the source system table structures "as-is," along with any additional metadata columns that capture the load date/time, process ID, etc. The focus in this layer is quick Change Data Capture and the ability to provide an historical archive of source (cold storage), data lineage, auditability, reprocessing if needed without rereading the data from the source system.

### Silver Layer (Cleansed and Conformed Data)

In the Silver layer of the lakehouse, the data from the Bronze layer is matched, merged, conformed and cleansed ("just-enough") so that the Silver layer can provide an "Enterprise view" of all its key business entities, concepts and transactions. (e.g. master customers, stores, non-duplicated transactions and cross-reference tables).

The Silver layer brings the data from different sources into an Enterprise view and enables self-service analytics for ad-hoc reporting, advanced analytics and ML. It serves as a source for Departmental Analysts, Data Engineers and Data Scientists to further create projects and analysis to answer business problems via enterprise and departmental data projects in the Gold Layer.

In the lakehouse data engineering paradigm, typically the ELT methodology is followed vs. ETL - which means only minimal or "just-enough" transformations and data cleansing rules are applied while loading the Silver layer. Speed and agility to ingest and deliver the data in the data lake is prioritized, and a lot of project-specific complex transformations and business rules are applied while loading the data from the Silver to Gold layer. From a data modeling perspective, the Silver Layer has more 3rd-Normal Form like data models. Data Vault-like, write-performant data models can be used in this layer.

### Gold Layer (Curated Business-Level Tables)

Data in the Gold layer of the lakehouse is typically organized in consumption-ready "project-specific" databases. The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins. The final layer of data transformations and data quality rules are applied here. Final presentation layer of projects such as Customer Analytics, Product Quality Analytics, Inventory Analytics, Customer Segmentation, Product Recommendations, Marking/Sales Analytics etc. fit in this layer. We see a lot of Kimball style star schema-based data models or Inmon style Data marts fit in this Gold Layer of the lakehouse.

So you can see that the data is curated as it moves through the different layers of a lakehouse. In some cases, we also see that lot of Data Marts and EDWs from the traditional RDBMS technology stack are ingested into the lakehouse, so that for the first time Enterprises can do "pan-EDW" advanced analytics and ML - which was just not possible or too cost prohibitive to do on a traditional stack. (e.g. IoT/Manufacturing data is tied with Sales and Marketing data for defect analysis or health care genomics, EMR/HL7 clinical data markets are tied with financial claims data to create a Healthcare Data Lake for timely and improved patient care analytics.)

Go to [Table of Contents](#table-of-contents)

## Implementation

### Report 1 (Operations Management Report)

#### [Generate Report 1](https://colab.research.google.com/github/jaison080/cockpit-manufacturing-data-analysis/blob/master/reports/report1.ipynb)

<ol>
<li>Spark session is created using the AWS Access Key and AWS Secret Key.</li>
<li>

Two schemas, `resultsSchema` and `workOrdersSchema` are defined.</li>

<li>

`PlansShiftWise.csv` is loaded into dataframe `plans_df`. It is a bronze layer data. Set `bronze_plans_df = plans_df`. Here the data is separated using delimiter `|`.</li>

<li>

Null columns are removed to make the dataframe `silver_plans_df `.</li>

<li>

`Results.csv` is loaded into dataframe `result_df` based on the schema `resultsSchema`. It is a bronze layer data. Set `bronze_result_df = result_df`. Here the data is separated using delimiter `,`.</li>

<li>

Null columns are removed to make the dataframe ``silver_result_df`.</li>

<li>

`RoutingStages.parquet` is loaded into dataframe `routing_df` . It is a bronze layer data.So `bronze_routing_df = routing_df`. Null columns are removed to make the dataframe `silver_routing_df`.</li>

<li>

Create `combined_df` as join of `work_orders_df`, `combined_df` where `WorkOrderId == work_orders.id` . Drop columns `RoutingStageName`, `BatchID`, `id`.</li>

<li>

Create dataframe `work_orders_df` based on the schema `workOrdersSchema` and load the given data `Workorders.csv`. Here the data is separated using delimiter `\t`.</li>

<li>

Drop columns `EcnNo`, `EcnQunatity`, `EcnStatus`,`ProductRevision`,`PlannedStartDate`, `PlannedEndDate`, `BlockedDate`, `BlockedBy`, `CreationDate`, `DysonPONumber`, `CustomerSKUNumber`, `Company`, `Division` from `work_orderst_df`.</li>

<li>

Set `combined_df` as join of `work_orders_df`, `combined_df` where `WorkOrderId == work_orders_df.Id`. Filter by `combined_df.Surface == 1`, and drop column `Id`.</li>

<li>

Create dataframe `actual_df` as `combined_df` grouped by the columns `ItemId`, `SubWorkCenter` according to the count of `BoardId` along with last modified date, time as `Hour` and `Date`.</li>

<li>

Set `combined_df` as join of `plans_df`, where `(actual_df.ItemId == plans_df.ItemNo) &  (actual_df.SubWorkCenter == plans_df.Station) & (actual_df.Hour == F.hour(plans_df.Hour)) & (actual_df.Date == F.date_format(plans_df.Date, "yyyy-MM-dd"))`. Drop `actual_df.Hour` and `actual_df.Date`.</li>

<li>

Create dataframe `items_df` and load the given data `Items.txt`. It is a bronze layer data. Set `bronze_items_df = items_df`. </li>

<li>Create pattern which is a regex expression is a sequence of different characters which describe the particular search pattern. </li>
<li>

Create schema called `itemsSchema` as given.</li>

<li>

Define function `extract_values` to extract data from input dataframe with the created pattern.</li>

<li>

Create dataframe `extract_values_udf` and extract values into it according to the created `itemsSchema`.</li>

<li>

In `items_df` create column `structured_data` and load value from `extract_values_udf`.</li>

<li>

Create columns in `items_df` as,
`items_df = items_df.withColumn("ID_Items", F.col("structured_data.ID"))`</li>

<li>Continue this operation for all columns.</li>
<li>

From `items_df` drop column `structured_data`.</li>

<li>

Now items_df is a silver layer data, Create `silver_items_df = items_df`.</li>

<li>

Remove the rows with null values by `items_df=items_df.na.drop()`.</li>

<li>

Create `final_df` as join of `items_df`, `combined_df` where `ItemId == items_df.ID_Items`. This is gold layer data. Set `gold_df = final_df`.</li>

<li>

Create grouped_data as `final_df.groupBy(“Hour”, “Date”, “ItemNo” `.</li>

<li>

Create `actual_production` which is the sum of column `ActualQuantity`.</li>

<li>

Create `planned_production` which is the sum of column `Quantity`.</li>

<li>

Planned production vs actual production is shown by dataframe which shows `actual_production`, `planned_production`, `Hour`, `Date`, `ItemNo`, and a new column called `Difference` which is , `F.col("Planned Production") - F.col("Actual Production")`.

</li>
<li>

Remove the rows with null values as `final_df=final_df.na.drop()`.</li>

<li>

Set `df_maxdate` as column date of `final_df` Sorted in descending order at a limit of 9. Set date_val as first value in in `df_maxdate`.</li>

<li>

Set `final_df = final_df.join(df_maxdate, on=["Date"])` and filter it by `col “date” >=date_val` and group by `Date`, `ItemId`, `Description` along with sum of `ActualQuantity`.</li>

<li>

Create window named `window` which helps to change a column in the dataframe by preserving the other columns.

`window = Window.partitionBy("Date").orderBy(F.col("sum(ActualQuantity)").desc())`.

 </li>
<li>

Change `final_df = final_df.select("*", F.row_number().over(window)alias("row_num"))`.</li>

<li>

Filter first_10_rows as `row_num` <=10.</li>

<li>

Set `first_10_rows = first_10_rows.drop("row_num")` sorted by descending order of `Date` at a limit of 70.</li>

</ol>

Go to [Table of Contents](#table-of-contents)

### Report 2 (SC Supply Chain Report)

#### [Generate Report 2](https://colab.research.google.com/github/jaison080/cockpit-manufacturing-data-analysis/blob/master/reports/report2.ipynb)
<ol>
<li>

Spark Session is created using the AWS Access key and AWS Secret key
Two schemas, `itemSchema` and `warehouseSchema` are defined.</li>

<li>

`Item.parquet` is loaded into dataframe `bronze_item_df` . It is a bronze layer data. For every column in `item_df.columns`, set `item_df = item_df.withColumnRenamed(col, [f.name for f in itemSchema.fields if f.name != col][0])`. </li>

<li>

Null columns are removed to make the dataframe `silver_item_df` . Set `silver_item_df=item_df`.</li>

<li>

Create dataframe `warehouse_df` based on the schema `warehouseSchema` and load the given data `warehouse.csv`. Here the data is separated using delimiter `,`. Set `bronze_warehousedf = warehouse_df`

</li>
<li>

A column `Registering Date` is added to `warehouse_df` and null columns are removed to make the dataframe `silver_warehousedf` . Set `silver_warehousedf=warehouse_df`.

</li>
<li>

Create dataframe `production_df` and load the given data `production.txt`. Here the data is separated using delimiter `\t`. Set `bronze_productiondf = production_df`

</li>
<li>

Null columns are removed to make the dataframe `silver_productiondf` .Set `silver_productiondf=production_df`.

</li>
<li>

Create `df_1` as `warehouse_df.groupBy ("Lot No_", "Bin Code", "Item No_","Registering Date").agg( F.min("Registering Date").alias("min_registering_date"),F.sum("Quantity").alias("sum_quantity"),F.first("Zone Code").alias("first_zone_code"),F.datediff(F.current_date(), F.col("Registering Date")).alias("date_diff")).filter("sum_quantity > 0")`

 </li>
 <li>

Create `df_2` as `df_2 = item_df.filter("Production_BOM_No != ''").select("No", "Production_BOM_No").union( production_df.select("No_", "Production BOM No_")).distinct()`

</li>
<li>

Create `df_3` as `df_3 = df_1.join(df_2, df_1["Item No_"] == df_2["No"], "left").drop("No")`

</li>
<li>

Create `df_4` as `df_4 = df_3.join(item_df, df_3["Item No_"] == item_df["No"], "inner")`

</li>
<li>

Null columns are removed to make the dataframe `gold_df` .Set `gold_df=df_4`.

</lI>
<li>

Inventory value distributed across different categories is obtained by grouping `df_4` with `Item Disc_Group` and aggregating it with the sum of `Unit Price`.

</li>
<li>

Top 10 inventory categories wrt value is obtained by sorting `inventory_value_by_category` in ascending order.

</li>
<li>

Inventory aging is obtained by grouping `df_4` with `Age` and aggregating it with the sum of `Unit Price`.

</li>
<li>

Inventory value distributed across different bins is obtained by grouping `df_4` with `Bin Code` and aggregating it with the sum of `Unit Price`.

</li>
</ol>

Go to [Table of Contents](#table-of-contents)

### Report 3 (Component Temperature Realtime Report)

#### [Generate Report 3](https://colab.research.google.com/github/jaison080/cockpit-manufacturing-data-analysis/blob/master/reports/report3.ipynb)

<ol>
<li>Create Spark Session called "Report 3 : Component Temperature Realtime Report".</li>
 
<li>

Set up kafka consumer as :

```bash
kafkaParams={{"bootstrap_servers":"<kafka-url>"}}
topic = "IOTTemperatureStream01"
consumer = KafkaConsumer(topic, **kafkaParams)
```

 </li>
 <li>

Create a dataframe `df` with the schema.

```bash
        StructField("lane_number", StringType(), True),
        StructField("plant_name", StringType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("component_type", StringType(), True),
        StructField("component_manufacturer", StringType(), True),
```

<li>

Create a function called `filterData` which is required to show the temperature data of the last 10 mins and clear out the whole data when 30 mins is passed.</li>

<li>

In `filterData` set :

```bash
last_10_minutes = datetime.now() - timedelta(minutes=10)
last_30_minutes= datetime.now() - timedelta(minutes=30)
```

and filter the input dataframe within the last 30 minutes and last 10 minutes.

```bash
filtered_max_temp_data = dataframe.filter(dataframe.timestamp > last_30_minutes)
filtered_data = dataframe.filter(dataframe.timestamp > last_10_minutes)
```

<li>

Filter `filtered_data` with temperature > 50.

</li>
<li>

Generate `component_counts` as `filtered_data .groupBy ("component_type").count()`, which is used to show the count of components with temperature > 50.

 </li>
 <li>

Create two windows named `window2`, window which helps to change a
column in the dataframe by preserving the other columns.

```bash
window2 = Window.partitionBy("lane_number").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
window = Window.partitionBy("lane_number").orderBy(desc("temperature"))
```

 <li>

Add two columns `avg_value` and `variance` with mean of `temperature` over `window2` and variance of `temperature` over `window`.

</li>
<li>

Set

```bash
max_temp_per_lane = filtered_max_temp_data.withColumn("row_number", row_number().over(window))
max_temp_per_lane = max_temp_per_lane.filter(col("row_number") ==1)
```

 </li>
 <li>

Plot a graph of `max_temp_per_lane` by converting into `pandas()`.

<li>

Create a function called `process(rdd)` which convert the input dataframe rdd from JSON file into data which is set as `bronze_data`.

</li> 
<li>

It removes the null columns from the data.

```bash
data = data.filter(data["component_info"].isNotNull())
data = data.filter(data["timestamp"].isNotNull())
```

 </li>
<li>

If `data.count` > 0 then

```bash
data = data.withColumn("timestamp",when(col("timestamp").cast("double").isNotNull( ), col("timestamp").cast("double").cast("timestamp")).otherwise(col("timestamp")))
data = data.withColumn("component_manufacturer",                        data["component_info"]["component_manufacturer"])
data = data.withColumn( "component_type", data["component_info"]["component_type"])
data = data.drop("component_info")
```

 </li>
<li>

This data is input into `df` which is already defined with a specific schema.

`df=df.union(data)`

</li>
<li>

The formed data is silver data which is again refined by performing `filterData(df)`.

</li>
 <li>

Finally in the main program the `consumer` is initialized data is loaded as JSON data from the `record.value`.

```bash
messages = consumer.poll(1000)
    for tp, message in messages.items():
        for record in message:
            data = json.loads(record.value)
```

</li>
<li>

Process the data in function `process()` only if there is component_info in data .

```bash
if "component_info" in data and data["component_info"] and "component_type" in data["component_info"] and data["component_info"] ["component_type"] and data['temperature'] is not None:
   rdd = sc.parallelize([record.value.decode('utf-8')])
   process(rdd)
```

</li>
<li>
The required data is obtained along with the generated graph.
</li>
</ol>


Go to [Table of Contents](#table-of-contents)

## Unit Test Cases

<ol>
<li>
<b>pytest.fixture(scope="session")</b> : Using this pytest fixture means we can pass the spark session into every unit test without creating a new instance each time. Creating a new Spark session is an intensive process; using the same one will allow our tests to run more efficiently.
</li>
<br>
<li>
<b>Test data</b> : We are using a specific two-row example as a small dataset is all we require to prove the test case. As a result, we can create the DataFrame using lists. I suggest using a directory of test data files in CSV or JSON format if you require larger datasets.
</li>
<br>
<li>
<b>Expected data</b> : To validate the test case, we need to have desired results. Again, we use lists, but you may prefer to use some files.
</li>
<br>
<li>
<b>.collect()</b>: Spark has two types of operations, Transformations, and Actions. Transformations are added to the directed acyclic graph(DAG) and don’t return results until an Action is invoked. As our function doesn’t perform any actions, we need to call .collect() so that our transformation takes effect.
</li>
<br>
<li>
<b>Assertion</b> : To ensure our job code is performing as expected, we need to compare the results of our transformation against our desired data. Unfortunately, Spark doesn’t provide any DataFrame equality methods, so we will loop through row by row and compare our results to the expected data.
</li>
</ol>

Go to [Table of Contents](#table-of-contents)

## Scalability

It is a massive scale infrastructure, across our two largest and business Hadoop clusters, they have 10,000 nodes, with about 1 million CPU vcores and more than two petabytes of memory. Every day about 30,000 Spark applications run on their clusters. These Spark applications contribute to about 70% of our total costs for computer resource usage. And they’ve generated close to five petabytes of data to be shuffled daily. This massive scale infrastructure also grows very fast.

Go to [Table of Contents](#table-of-contents)

## Deployment

### Step 1: Download Spark Jar

Spark Core jar is required for compilation, therefore, download `spark-core_2.10-1.3.0.jar` from the following link <a href="http://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10/1.3.0">Spark Core jar</a> and move the jar file from download directory to spark-application directory.

### Step 2: Compile the code

Compile the above program using the command given below. This command should be executed from the spark-application directory. Here, `/usr/local/spark/lib/spark-assembly-1.4.0-hadoop2.6.0.jar` is a Hadoop support jar taken from Spark library.

```bash
scalac -classpath "spark-core_2.10-1.3.0.jar:/usr/local/spark/lib/spark-assembly-1.4.0-hadoop2.6.0.jar" SparkPi.scala
```

### Step 3: Create a jar file

Create a jar file of the spark application using the following command. Here, wordcount is the file name for jar file.

```bash
jar -cvf wordcount.jar SparkWordCount*.class spark-core_2.10-1.3.0.jar/usr/local/spark/lib/spark-assembly-1.4.0-hadoop2.6.0.jar
```

### Step 4: Submit the Spark Application

Submit the spark application using the following command :

```bash
spark-submit --class SparkWordCount --master local wordcount.jar
```

Go to [Table of Contents](#table-of-contents)

## Handling Exceptions

There are two ways to handle exceptions. One using an accumulator to gather all the exceptions and report it after the computations are over. The second option is to have the exceptions as a separate column in the data frame stored as String, which can be later analysed or filtered, by other transformations.

If the number of exceptions that can occur are minimal compared to success cases, using an accumulator is a good option, however for large number of failed cases, an accumulator would be slower.

Go to [Table of Contents](#table-of-contents)

## Conclusion

In this project, we have implemented a real-time streaming application using Apache Spark. We have used Kafka as a message broker to stream data from the producer to the consumer. We have used Spark Streaming to process the data in real-time. We have used Spark SQL to perform data analysis and data transformation. We have used Bokeh to generate a graph.

Go to [Table of Contents](#table-of-contents)

## References

1. <a href="https://spark.apache.org/docs/latest/sql-programming-guide.html">Spark SQL, DataFrames and Datasets Guide</a>
2. <a href="https://spark.apache.org/docs/latest/quick-start.html">Spark Quick Start</a>
3. <a href="https://spark.apache.org/docs/latest/submitting-applications.html">Submitting Applications</a>
4. <a href="https://spark.apache.org/docs/latest/configuration.html">Spark Configuration</a>
5. <a href="https://spark.apache.org/docs/latest/monitoring.html">Spark Monitoring</a>

Go to [Table of Contents](#table-of-contents)
