import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

# DROP TABLES

Capitals_STG_table_drop = "DROP TABLE IF EXISTS Capitals_STG"
CostofLiving_STG_table_drop = "DROP TABLE IF EXISTS CostofLiving_STG"
WorldHappinesssReport_STG_table_drop = "DROP TABLE IF EXISTS WorldHappinesssReport_STG"
Capitals_table_drop = "DROP TABLE IF EXISTS Capitals"
CostofLiving_table_drop = "DROP TABLE IF EXISTS CostofLiving"
WorldHappinesssReport_table_drop = "DROP TABLE IF EXISTS WorldHappinesssReport"

# CREATE TABLES

Capitals_STG_table_create= ("""Create table if not exists Capitals_STG(
Country VARCHAR(500),
City VARCHAR(500),
Latitude VARCHAR(200),
Longitude VARCHAR(200),
CountryCode VARCHAR(2),
Region VARCHAR(200)
)
""")

CostofLiving_STG_table_create = ("""Create table if not exists CostofLiving_STG(
country VARCHAR(500),
City VARCHAR(500),
cost_of_living_index VARCHAR(200),
Rent_Index VARCHAR(200),
Cost_of_living_plus_rent_index VARCHAR(200),
Groceries_index VARCHAR(200),
Restaurant_price_index VARCHAR(200),
Local_purchasing_power_index VARCHAR(200),
year int
)
""")

WorldHappinesssReport_STG_table_create = ("""Create table if not exists WorldHappinesssReport_STG(
country VARCHAR(500),
region VARCHAR(200),
Happiness_Rank VARCHAR(200),
happiness_score VARCHAR(200),
GDP_Per_Capita VARCHAR(200),
year int
)
""")

Capitals_table_create = ("""Create table if not exists Capitals(
Country VARCHAR(500),
City VARCHAR(500),
Latitude VARCHAR(200),
Longitude VARCHAR(200),
CountryCode VARCHAR(2),
Region VARCHAR(200)
)
""")

CostofLiving_table_create = ("""Create table if not exists CostofLiving(
country VARCHAR(500),
City VARCHAR(500),
cost_of_living_index VARCHAR(200),
Rent_Index VARCHAR(200),
Cost_of_living_plus_rent_index VARCHAR(200),
Groceries_index VARCHAR(200),
Restaurant_price_index VARCHAR(200),
Local_purchasing_power_index VARCHAR(200),
year int
)
""")

WorldHappinesssReport_table_create = ("""Create table if not exists WorldHappinesssReport(
country VARCHAR(500),
region VARCHAR(200),
Happiness_Rank VARCHAR(200),
happiness_score VARCHAR(200),
GDP_Per_Capita VARCHAR(200),
year int
)
""")


# STAGING TABLES


Capitals_STG_copy=("""COPY Capitals_STG from {}\
					ACCESS_KEY_ID '{}'\
					SECRET_ACCESS_KEY '{}'\
					CSV QUOTE AS '"'\
					IGNOREHEADER 1\
					DELIMITER ','\
					TRUNCATECOLUMNS""").format(config.get("S3","CAPITALS"),config.get("AWS","AWS_ACCESS_KEY_ID"),config.get("AWS","AWS_SECRET_ACCESS_KEY"))

CostofLiving_STG_copy=("""COPY CostofLiving_STG from {}\
					ACCESS_KEY_ID '{}'\
					SECRET_ACCESS_KEY '{}'\
					CSV QUOTE AS '"'\
					IGNOREHEADER 1\
					DELIMITER ','\
					TRUNCATECOLUMNS""").format(config.get("S3","COSTOFLIVING"),config.get("AWS","AWS_ACCESS_KEY_ID"),config.get("AWS","AWS_SECRET_ACCESS_KEY"))

WorldHappinesssReport_STG_copy = ("""copy WorldHappinesssReport_STG from {}\
                       ACCESS_KEY_ID '{}'\
					   SECRET_ACCESS_KEY '{}'\
					   FORMAT AS JSON 'auto'""").format(config.get("S3","WORLDHAPPINESS"),config.get("AWS","AWS_ACCESS_KEY_ID"),config.get("AWS","AWS_SECRET_ACCESS_KEY"))

# FINAL TABLES

Capitals_table_insert = (""" Insert into Capitals(Country, City, Latitude, Longitude, CountryCode, Region)
select cp.Country, cp.City, cp.Latitude, cp.Longitude, cp.CountryCode, cp.Region from Capitals_STG cp
where cp.Country is not null
""")

CostofLiving_table_insert = (""" insert into CostofLiving(country, City, cost_of_living_index, Rent_Index, Cost_of_living_plus_rent_index, Groceries_index,Restaurant_price_index,Local_purchasing_power_index,year)
select cl.country, cl.City, cl.cost_of_living_index, cl.Rent_Index, cl.Cost_of_living_plus_rent_index, cl.Groceries_index,cl.Restaurant_price_index,cl.Local_purchasing_power_index,cl.year  from CostofLiving_STG cl
where cl.Country is not null
""")

WorldHappinessReport_table_insert = ("""insert into WorldHappinesssReport(country, region, Happiness_Rank,happiness_score, GDP_Per_Capita,year)
select whr.country, whr.region, whr.Happiness_Rank, whr.happiness_score, whr.GDP_Per_Capita,whr.year from WorldHappinesssReport_STG whr
where whr.Country is not null
""")

# QUERY LISTS

create_table_queries = [Capitals_STG_table_create, CostofLiving_STG_table_create, WorldHappinesssReport_STG_table_create, Capitals_table_create, CostofLiving_table_create, WorldHappinesssReport_table_create]
drop_table_queries = [Capitals_STG_table_drop, CostofLiving_STG_table_drop, WorldHappinesssReport_STG_table_drop, Capitals_table_drop, CostofLiving_table_drop, WorldHappinesssReport_table_drop]
copy_table_queries = [Capitals_STG_copy, CostofLiving_STG_copy,WorldHappinesssReport_STG_copy]
insert_table_queries = [Capitals_table_insert, CostofLiving_table_insert, WorldHappinessReport_table_insert]