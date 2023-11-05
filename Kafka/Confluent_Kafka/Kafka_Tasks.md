# QUESTIONS
## 1.  
Create a topic named `atscale`, 2 partitions and replication factor 1.

docker exec -it kafka1 bash

/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic atscale \
--replication-factor 1 \
--partitions 2

/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

PS, Delete command:
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic topic_name

## 2. 
List all topics:

/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list


## 3. 
Describe `atscale` topic.

FOR DESCRIBE:
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic atscale



## 4. 
Use data-generator and send `https://raw.githubusercontent.com/erkansirin78/datasets/master/Churn_Modelling.csv` to  3 patitioned `churn` topic.


- Message key should be CustomerId.

- Consume under `churn_group` and this group must have 3 consumer. 
    - Use different terminal for each consumer. 
    - Use `kafka-console-consumer.sh` as a consumer.

You can check the group list:
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

-------------------------------------------

    source datagen/bin/activate
    python dataframe_to_kafka.py -i ./input/Churn_Modelling.csv -t churn -k 1

    X3 because the task request is 3 consumer. kafka1, kafka2, kafka3
    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic churn --group churn_group


## 5. 
Delete `atscale` and `churn` topics.

BE CAREFUL!! IF THE PARTITIONS ARE RUNNIG BACKGROUND, THE COMMAND OF DELETE DON'T WORK!

/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic atscale
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic churn


## 6.
-  Using Python Kafka do the following tasks:
    - Produce the names of Turkey's geographical regions to a topic you specify, using the numbers you specify at the beginning of each of them as keys. For example, 1 Marmara, 2 Aegean.
    - With the Consumer, print the key, value, partition, timestamp information as following example.
```
Key: 1, Value: Marmara, Partition: 0, TS: 1613224639352 
Key: 4, Value: İç Anadolu, Partition: 1, TS: 1613224654849 
Key: 3, Value: Akdeniz, Partition: 2, TS: 1613224661486 
Key: 2, Value: Ege, Partition: 2, TS: 1613224667044
```

conda activate kafkaenv

python3 consumer.py

python3 producer.py

## 7.
- Truncate topic1.
- Produce iris.csv using data-generator to topic1.
- Build a python consumer;
	- Comsume from topic1. 
	- Write the message content, topic name, partition number of each flower type in a separate file with its own name (`/tmp/kafka_out/<species_name_out.txt`>).
	- Write messages that do not belong to any of the three flower types in the `/tmp/kafka_out/other_out.txt` file.

Example result file tree: 
```
tree /tmp/kafka_out/
/tmp/kafka_out/
├── other_out.txt
├── setosa_out.txt
├── versicolor_out.txt
└── virginica_out.txt
```

Example file content
```
 head /tmp/kafka_out/setosa_out.txt
topic1|2|0|0|5.1,3.5,1.4,0.2,Iris-setosa
topic1|2|1|2|4.7,3.2,1.3,0.2,Iris-setosa
topic1|2|2|3|4.6,3.1,1.5,0.2,Iris-setosa
topic1|2|3|9|4.9,3.1,1.5,0.1,Iris-setosa
topic1|2|4|16|5.4,3.9,1.3,0.4,Iris-setosa
topic1|2|5|29|4.7,3.2,1.6,0.2,Iris-setosa
topic1|2|6|32|5.2,4.1,1.5,0.1,Iris-setosa
topic1|2|7|36|5.5,3.5,1.3,0.2,Iris-setosa
topic1|2|8|40|5.0,3.5,1.3,0.3,Iris-setosa
topic1|2|9|41|4.5,2.3,1.3,0.3,Iris-setosa
```
START READER:
conda activate kafkaenv
python3 consumer.py

THEN START DATA-GEN:
/home/maktas/data-generator
source datagen/bin/activate
python3 dataframe_to_kafka.py -t topic1


## 8.
Write a python function that fulfills the following requirements.
- Arguments: KafkaAdminClient object, topic name, number of partitions and replication factor
- Do not take any action if there is a topic with the same name
- If there is no topic with the same name, it will create a topic using arguments

def create_a_new_topic_if_not_exists(admin_client, topic_name="example-topic", num_partitions=1, replication_factor=1):
	<YOUR CODE HERE>


/home/maktas/VBO_HomeWork/apache_kafka/zookeeperless_kafka/eighttask
conda activate kafkaenv
python3 py_kafka_eight_task.py

docker exec -it kafka1 bash
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
