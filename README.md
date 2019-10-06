## Data Engineering Capstone Project

### Summary:
   This is final project of Udacity Data Engineering Nanodegree Program. The Capstone Project aim is to Design, Model, analyze and build ETL & data pipeline using public data-sets.

### Step 1: Scope the Project and Gather Data

This project reports a high level analysis of top 10 countries happiness index and their cost of living. Both the happiness  and cost of living datasets are downloaded from Kaggle. Happiness dataset contains happiness index and rankings from countries all over the world based on the content of six factors – economic production, social support, life expectancy, freedom, absence of corruption, and generosity.

Exploratory Data Analysis performed on the datasets, data consolidation from various datasets, data has been cleansed, loaded into S3 and then to Redshift, finally here in this notebook we will analyse the cost of living in the happiest countries.

### Datasets:

1)  **World happiness reports**  from Kaggle. The datasets are in  **CSV**  file format.  Since we need to work on JSON format source, I have cleansed below csv files and merged into 1 single file and converted it to JSON.

-   **[World Happiness Report 2015 - 2017](https://www.kaggle.com/unsdsn/world-happiness)**      
-   **[World Happiness Report 2018](https://www.kaggle.com/njlow1202/world-happiness-report-data-2018)**      
    > **Columns:**  Country, Happiness rank, Happiness score, Economy Index (GDP per Capita)

2)  **Cost of Living**  - from Kaggle are in  **CSV**  file format.  

-   **[Cost of living Index 2016 - 2018](https://www.kaggle.com/andytran11996/cost-of-living)**  
    
    > To find out out how Singapore fare against those countries, we will convert the indices to be  `relative to Singapore`  instead of New York City after reading them from the csv files.  
    >   
    > **Columns:**  
    > 
    > -   City : This reports contains data of major cities in each countries. To simplify the matters, we only take the capital cities as the measure.
    > -   Country : Countries from world wide.
    > -   Cost of Living Index (Excl. Rent) :  _is a relative indicator compared to New York of consumer goods prices, including groceries, restaurants, transportation and utilities, but doesn't include accommodation expenses such as rent or mortgage._  
    >     
    > -   Rent Index :  _is an estimation of prices of renting apartments in the city compared to New York City._  
    >     
    > -   Groceries Index :  _is an estimation of grocery prices in the city compared to New York City._  
    >     
    > -   Restaurants Index :  _is a comparison of prices of meals and drinks in restaurants and bars compared to New York City._  
    >     
    > -   Cost of Living Plus Rent Index :  _is an estimation of consumer goods prices including rent comparing to New York City._  
    >     
    > -   Local Purchasing Power :  _shows relative purchasing power in buying goods and services in a given city for the average wage in that city. If domestic purchasing power is 40, this means that the inhabitants of that city with the average salary can afford to buy on an average 60% less goods and services than New York City residents with an average salary._  
    >     
    

3) List of countries and capital cities from  **[Kaggle - World capitals gps.](http://techslides.com/list-of-countries-and-capitals)**

### Tools Used:

 - Explore and extract data -- `Python notebooks, Pandas`
 - Staging  and Storing the data -- `Amazon S3, Redshift`

### Step 2: Explore and Assess the Data:
Data consolidation from datasets  
1) Both world happiness and cost of living datasets are in separate csv files by years.  
 - Take a peek into each datasets to determine the data to extract.  
 - Consolidate these datasets into dataframe for further cleaning process. Standardized the columns during consolidation.  
2) Merged files into 1 single CSV/JSON file and upload them to S3.
all the data analysis is in Exploratory data analysis notebook.

### Step 3: Define the Data Model:

After Exploring/Analyzing the data. The star schema model is chosen.  Since we have only 3 tables and all are dimensional tables I'm not creating any design model.

### Step 4: Run Pipelines to Model the Data

Project contains below files
 - dl.cfg --config file which has all AWS credentials.
 - Redshift_cluster.py -- Creates redshift cluster.
 - etl.py --This ETL pipeline script copies local files to S3 and the sent to Redshift.
#### Run ETL pipeline

1. First run `python Redshift_cluster.py` and create redshift cluster.
2. Then run below the ETL  `python etl.py` to upload local files to S3 and copy S3 data to Redshift.
3. During the execution, ETL script prints out information about the progress of the script execution.
4. Analysed processed data in Cost of living in the world happiest country notebook.

### Step 5: Complete Project Write Up

If I had 100x times then I would have to set up a spark cluster like with AWS EMR.

Also this data set is a one time file, but in a real case with data that would come every day from report I would setup a daily workflow with AirFlow and split all the logic included in this notebook in several task in a  `@daily`  DAG.

A hadoop cluster with Spark SQL on top would be also a good fit if 100 persons in a company had to work with the data since it would scale automatically depending of the CPU consumption.

 
