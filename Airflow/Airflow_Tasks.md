# Preparation for homework
Before starting homework, you need to do following prerequisites.

## Create containers
- Using docker-compose.yaml file create nginx container.

## Download data to nginx container
```commandline
# connect to nginx container
docker exec -it nginx bash

# change directory to html root
root@1ea647bbd0c4:/# cd /usr/share/nginx/html/
root@1ea647bbd0c4:/usr/share/nginx/html# mkdir data
root@1ea647bbd0c4:/usr/share/nginx/html# cd data

# Download data
root@1ea647bbd0c4:/usr/share/nginx/html/data# for i in {10..20}; do curl -o hour_${i}_supermarket_sales.csv  https://raw.githubusercontent.com/erkansirin78/datasets/master/supermarket_hourly/hour_${i}_supermarket_sales.csv; done

# check data
root@1ea647bbd0c4:/usr/share/nginx/html/data# ls -l
total 160
-rw-r--r--. 1 root root   497 Jul 19 14:05 50x.html
-rw-r--r--. 1 root root 13698 Oct 15 05:57 hour_10_supermarket_sales.csv
-rw-r--r--. 1 root root 12159 Oct 15 05:57 hour_11_supermarket_sales.csv
-rw-r--r--. 1 root root 12069 Oct 15 05:57 hour_12_supermarket_sales.csv
-rw-r--r--. 1 root root 13906 Oct 15 05:57 hour_13_supermarket_sales.csv
-rw-r--r--. 1 root root 11241 Oct 15 05:57 hour_14_supermarket_sales.csv
-rw-r--r--. 1 root root 13770 Oct 15 05:57 hour_15_supermarket_sales.csv
-rw-r--r--. 1 root root 10454 Oct 15 05:57 hour_16_supermarket_sales.csv
-rw-r--r--. 1 root root 10085 Oct 15 05:57 hour_17_supermarket_sales.csv
-rw-r--r--. 1 root root 12597 Oct 15 05:57 hour_18_supermarket_sales.csv
-rw-r--r--. 1 root root 15266 Oct 15 05:57 hour_19_supermarket_sales.csv
-rw-r--r--. 1 root root 10222 Oct 15 05:57 hour_20_supermarket_sales.csv
-rw-r--r--. 1 root root   615 Jul 19 14:05 index.html
```

## Exit from nginx container

## Test api
```commandline
[train@trainvm ~]$ curl -o /tmp/hour_13_supermarket_sales.csv http://localhost:5000/data/hour_13_supermarket_sales.csv

[train@trainvm ~]$ ls -l /tmp | grep super
-rw-rw-r--. 1 train train  13906 Oct 15 09:02 hour_13_supermarket_sales.csv
```


# Homework
## Business requirements
- New sales are available per hour in `http://localhost:5000/data`
- You need to ingest this data into object storage as it is. 
  - Example:
    - For the time xx:yy you need to process hour_xx_supermarket_sales.csv file.
    - For the time 13:15 you need to process hour_13_supermarket_sales.csv file.
- For some hours there will be no data. In this case your pipeline must not fail. 
- After ingesting data from nginx server you need to save object storage:
  - Bucket: `dataops`
  - Key: `supermarket_sales/{today}/hour_{this_hour}_supermarket_sales.csv`
- For each day there must be different directory and in this directory there must be at most 24 files for each hour. But if there is no data in the source, no file is acceptable.
- Example value for today: 20230312 and example value for hour: 17


