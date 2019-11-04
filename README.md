## Data Engineering Capstone Project
*Please refer to [Capstone Project.ipynb](https://github.com/paulXLV/UdacityDataEngineeringCapstone/blob/master/Capstone%20Project.ipynb) to see full code explanation and data outputs.

##### Project Summary 

In our research company, Data Scientists are tasked to observe tourism behaviors and called on the Data Engineers to clean, process and develop data model (star schema) that would be the starting point of long-term project (of more data collection and experimenting) that will allow them to hypothesis relationships or patterns between the cities that non-immigrants visited and the cities demographics. 
We will create dimensional (and fact) tables and saved as parquet files for star schema model on I94 non-immigrants ports of entries data and US port city demographics data which will be a data model for queries on non-immigrants entering US to observe relationships between non-immigrants profiles (age, country of origin, seasons or holidays visited, reason for visiting, etc) and the cities demographics.

The project overview steps:

- ### Step 1: Scope the Project and Gather Data
###### The I94 immigration data comes from the US National Tourism and Trade Office. This data was already provided inside sas_data folder in parquet file type. We will use the following records:

1) i94Bir - Age of non-immigrant in years

2) admnum - Admission Number

3) i94res - 3 digit code of nationality

4) i94port - 3 character code of destination USA city

5) arrdate - arrival date in the USA (SAS date numeric field)

6) i94mode - 1 digit mode (plane, boat, etc) of travel code

7) i94visa - reason for immigration

8) gender - Non-immigrant sex

9) depdate - departure date in the USA (SAS date numeric field)

10) count - Used for summary statistics

###### U.S. City Demographic Data comes from OpenSoft in csv file. We will use the following records:

1) City (USA)

2) Male Population

3) Female Population

4) Median Age (overall median age within the city population)

5) Total Population

6) Foreign-Born (number of foreign born residences)

7) State Code (USA state abbreviation of the City column)

8) Race (specifies race category suchas Asian, Alaskan Indian, Black, Hispanic, etc)

9) Count (number of people under specific race category anotated by Race column)
- ### Step 2: Explore and Assess the Data
###### US CITIES DEMOGRAPHICS DATA SET (CLEANING AND TRANSFORMATION) 
Transform the US Demo dataset by doing a pivot on the Race column to convert Race categories into individual columns.
We will accomplish this transformation by separating both Race and Count columns from the US demo dataset and create two separate datasets called US and US RACE COUNT.

US RACE COUNT dataset will also include City and State Code columns so we can use them to join back with US dataset. Before we join the two datasets we first remove duplicate rows from the US dataset and pivot the US Race Count dataset. In theory both datasets should have equal rows to join them back to single transformed table.

*Please refer to [Capstone Project.ipynb](https://github.com/paulXLV/UdacityDataEngineeringCapstone/blob/master/Capstone%20Project.ipynb) for more explanations of cleaning all data sets required for this project.
 
- ### Step 3: Define the Data Model
Using the dimensional tables saved as parquet files we can implement them on any columnar database in Star Schema model.
Star Schema model was chosen because it will be easier for Data Analysts and Data Scientists
to understand and apply queries with best performance outcomes and flexibility.

##### U.S. Immigration and U.S. Ports Cities Demographics Data Model
![](/i94star_schema2.png)
##### Map Out Data Pipelines
Steps necessary to pipeline the data into the chosen data model

Install the packages:

- pandas
- datetime
- pyspark.sql -&gt; SparkSession -&gt; "org.apache.hadoop:hadoop-aws:2.7.0"
- pyspark.sql.functions -&gt; first, upper, col, udf, date_format, expr
- pyspark.sql.types -&gt; StructField, StructType, StringType, LongType, IntegerType

1. Create the dimensions (i94port, i94visa, i94res, i94mode) from i94_SAS_Labels_Descriptions.SAS file.
*NOTE: Once they're created it does not have to be included in future Data Pipeline schedules because these are essentially master records which do not frequently get added or changed on the dimension tables.

2. Read US Cities Demo dataset file to form us_spark dataframe 
3. Create 'us_race_cnt' from us_spark 
4. Drop columns we don't need and drop duplicate rows from us_spark 
5. Join us_spark with us_race_cnt to form US data set 
6. Change state code column name to state_code and other similar problems to avoid parquet complications 
7. Drop the state column 
8. Write (and overwrite) transformed US dataset onto parquet file 
9. Read i94 non-immigration dataset to form i94_spark dataframe 
10. Convert numbers to longtype and integertype 
11. Drop duplicate rows 
12. Read i94port dimension parquet file so we can use it to join with i94_spark. This will add i94port city and state columns to i94_spark dataframe 
13. Drop id column from i94_spark dataframe 
14. Join US with i94_spark to get fact table i94non_immigrant_port_entry 
15. Add iso date format column arrival_date inside the i94non_immigrant_port_entry dataframe by using custom function. 
16. Create time dimension from i94non_immigrant_port_entry and save to parquet file. 
17. Drop arrival_date column from i94non_immigrant_port_entry and save it to parquet file. 
18. Add seasons to i94date_seasons dataframe. 
19. Save i94date_seasons to parquet file partitioned by year and month.

- ### Step 4: Run ETL to Model the Data
The data pipeline is built inside the ETL.py file included with this Capstone Project.

- ### Step 5: Complete Project Write Up
- Clearly state the rationale for the choice of tools and technologies for the project?

Because we are dealing with big data and cloud technologies for solutions it made economical sense to use opensource Apache PySpark and Python tools that can be easily ported over to cloud solution such as AWS.  
- Propose how often the data should be updated and why?

Dimenstion tables only have to be updated when a new category is created by I94. However, the I94 non-immigrant port of entry data along with the time dimension table (i94date) can be updated every month. The US Cities Demographics data is updated every ten years according to [https://www.usa.gov/statistics](https://www.usa.gov/statistics). So, the new US Cities Demographics data set maybe coming after year 2020. And may need updating after one year or two years as of 2019.  
- Write a description of how you would approach the problem differently under the following scenarios:
    - The data was increased by 100x?
    
    Deploy this Spark solution on a cluster using AWS (EMR cluster) and use S3 for data and parquet file storage. AWS will easily scale when data increases by 100x  
    - The data populates a dashboard that must be updated on a daily basis by 7am every day?
    
    Use Apache Airflow to schedule queries.  
    - The database needed to be accessed by 100+ people?
    
    The saved parquet files can be bulk copied over to AWS Redshift cluster where it can scale big data requirements and has 'massively parallel' and 'limitless concurrency' for thousands of concurrent queries executed by users.
