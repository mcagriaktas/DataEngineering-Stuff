# Questions
if containers are not runnig, use:
docker-compose up -d

NOTE: Don't forget to start services.
    docker exec -it cluster-master "/usr/local/hadoop/spark-services.sh"

docker exec -it cluster-master bash

### Q-1: 
- Create a hive database `hive_odev` and load this data https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv into `wine` table.

hdfs dfs -mkdir -p /user/root/datasets

Don't forget, you're same path with Wine.csv!!!!!!!
hdfs dfs -put Wine.csv /user/root/datasets

start hive:
hive

CREATE TABLE IF NOT EXISTS hive_odev.wine (
  Alcohol FLOAT,
  Malic_Acid FLOAT,
  Ash FLOAT,
  Ash_Alcanity FLOAT,
  Magnesium FLOAT,
  Total_Phenols FLOAT,
  Flavanoids FLOAT,
  Nonflavanoid_Phenols FLOAT,
  Proanthocyanins FLOAT,
  Color_Intensity FLOAT,
  Hue FLOAT,
  OD280 FLOAT,
  Proline FLOAT,
  Customer_Segment FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

### Q-2
- In `wine` table filter records that `Alcohol`greater than 13.00 then insert these records into `wine_alc_gt_13` table.

load data inpath '/user/root/datasets/Wine.csv' into table hive_odev.wine;

INSERT:

CREATE TABLE IF NOT EXISTS hive_odev.wine_alc_gt_13 (
  Alcohol FLOAT,
  Malic_Acid FLOAT,
  Ash FLOAT,
  Ash_Alcanity FLOAT,
  Magnesium FLOAT,
  Total_Phenols FLOAT,
  Flavanoids FLOAT,
  Nonflavanoid_Phenols FLOAT,
  Proanthocyanins FLOAT,
  Color_Intensity FLOAT,
  Hue FLOAT,
  OD280 FLOAT,
  Proline FLOAT,
  Customer_Segment FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


BE CAREFUL, second table name is hive_odev.wine_alc_GET_13!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

INSERT INTO wine_alc_gt_13 
SELECT *
FROM hive_odev.wine
WHERE Alcohol > 13.00;

check the table:

select * from wine_odev.wine_alc_gt_13 where alcohol < 13;

if you want to see the table in Hadoop, you can check your warehouse in hdfs:

hdfs dfs -cat /user/hive/warehouse/hive_odev.db/wine_alc_gt_13/000000_0

### Q-3
- Drop `hive_odev` database including underlying tables in a single command.

DROP DATABASE IF EXISTS hive_odev CASCADE;

if you want to delete all tables in our database except our database,

DROP TABLE IF EXISTS hive_odev.*;

### Q-4 
- Load this https://raw.githubusercontent.com/erkansirin78/datasets/master/hive/employee.txt into table `employee` in `company` database. 

hdfs dfs -put employee.txt /user/root/datasets

go hive:

CREATE TABLE IF NOT EXISTS company.employee (
  name STRING,
  work_place STRING,
  gender_age STRING,
  skills_score STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties('skip.header.line.count'='1');

load data inpath '/user/root/datasets/employee.txt' into table company.employee;


### Q-5
- Write a query that returns the employees whose Python skill is greater than 70.

SELECT
  name,
  CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(skills_score, 'Python:', -1), ',', 1) AS INT) AS Python
FROM company.employee
WHERE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(skills_score, 'Python:', -1), ',', 1) AS INT) > 70;


CREATE TABLE IF NOT EXISTS company.employee_optimized (
  name STRING,
  work_place STRING,
  gender STRING,
  age INT,
  skill1 STRING,
  score1 int,
  skill2 STRING,
  score2 int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' 
MAP KEYS TERMINATED BY ':'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


INSERT INTO TABLE company.employee_optimized
SELECT
  name,
  CASE 
    WHEN size(split(work_place, ',')) > 1 THEN split(work_place, ',')[1]
    ELSE split(work_place, ',')[0]
  END AS working_place,
  split(gender_age, ',')[0] AS gender,
  CAST(split(gender_age, ',')[1] AS INT) AS age,
  regexp_replace(split(skills_score, ',')[0], '[0-9]', '') AS skill1,
  CAST(regexp_replace(split(skills_score, ',')[0], '[^0-9]', '') AS INT) AS score1,
  regexp_replace(split(skills_score, ',')[1], '[0-9]', '') AS skill2,
  CAST(regexp_replace(split(skills_score, ',')[1], '[^0-9]', '') AS INT) AS score2
FROM company.employee;











