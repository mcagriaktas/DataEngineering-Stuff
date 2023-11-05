from kafka import KafkaConsumer
import psycopg2


consumer = KafkaConsumer('churn',
                         consumer_timeout_ms=10000,
                         bootstrap_servers=['localhost:9092', 'localhost:9292'])

#ConnectDB

conn = psycopg2.connect(database="postgres", user='postgres', password='Ankara06', host='127.0.0.1', port= '5432')
cursor = conn.cursor()

#CreateTable
cursor.execute("DROP TABLE IF EXISTS ChurnCustomers")
sql ='''CREATE TABLE ChurnCustomers(
    CustomerId INT NOT NULL,
    Gender char(20),
    Age INT,
    EstimatedSalary FLOAT,
    Exited INT
)'''
cursor.execute(sql)
conn.commit()


for msg in consumer:
    messagekey = msg.key.decode("utf-8")
    messagevalue = msg.value.decode("utf-8")
    print(messagekey)
    print(messagevalue)
    print("*"*20)
    x=messagevalue.split(",")
    cursor.execute(
        "INSERT INTO ChurnCustomers (CustomerId, Gender, Age, EstimatedSalary, Exited) VALUES (%s,%s,%s,%s,%s)",
        (messagekey, str(x[0]), x[1], x[2], x[3]))
    conn.commit()
    print('Data added to POSTGRESQL')

conn.close()
