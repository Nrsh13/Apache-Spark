# Spark Structured Streaming Examples

## Purpose
To Process Input Streams from Sources like Socket, Directory and Kafka and send to Sinks like Console, File and Kafka using Strucutred Streaming.

## Prerequisites
```
Up and Running Hadoop Cluster with Apache Spark > 2.x
If you use Kafka as Source or Sink -
  Up and Running Kafka Cluster.
  Jars: spark-sql-kafka-0-10_<scalaVersion>-<SparkVersion>.jar and $KAFKA_HOME/libs/kafka-clients-2.0.0.jar
  Download Link:
    #1. wget http://central.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.3.1/spark-sql-kafka-0-10_2.11-2.3.1.jar
    #2. kafka-clients-2.0.0.jar can be found at $KAFKA_HOME/libs
```

## Description
Script will display all instructions like:
- How to start the Source say Kafka so that Spark can read it as a Stream.
- Sample data to be used as an input to the Source.
- Where to Check the Ouput and what command to use.
  
## Usage
```
[root@apache-spark ~]$ spark-submit socket-Dir-Kafka-Structured-Streaming.py help

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

          # For Socket and Directory Streaming:
                Usage: spark-submit socket-Dir-Kafka-Structured-Streaming.py <input-source-type> <sink-type>

          # For Kafka Streaming:
                Usage: spark-submit --jars spark-sql-kafka-0-10_<scalaVersion>-<sparkVersion>.jar,kafka-clients-2.0.0.jar socket-Dir-Kafka-Structured-Streaming.py <input-source-type> <sink-type>

          Where
                input-source-type: socket or directory or kafka
                sink-type: console or file or kafka

                Jar Needed: spark-sql-kafka-0-10_<scalaVersion>-<SparkVersion>.jar and $KAFKA_HOME/libs/kafka-clients-2.0.0.jar
                Download Link:
                        #1. wget http://central.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.3.1/spark-sql-kafka-0-10_2.11-2.3.1.jar
                        #2. kafka-clients-2.0.0.jar can be found at $KAFKA_HOME/libs
```

## Example
Console 1: Run the main Script and go through the Instructions provided.
```
[root@apache-spark ~]$  spark-submit --jars spark-sql-kafka-0-10_2.11-2.3.1.jar,$KAFKA_HOME/libs/kafka-clients-2.0.0.jar socket-Dir-Kafka-Structured-Streaming.py kafka kafka

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

INFO: Setting up Spark Streaming Environment

INFO: Use below Command to Start the Producer

       $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list apache-spark.hadoop.com:9092 --topic mytopic

INFO: Use below sample Data(For- Socket/Kafka) or DataFile(For- Directory/Flume) one by one as Input

       ALERT: *** Make Sure you use the same Format ***

# ROUND 1: To Add some records
{"fname" : "Billy","lname" : "Clark","email" : "Billy_Clark@@yahoo.com","principal" : "Billy@EXAMPLE.COM","passport_make_date" : "2013-07-30 14:39:59.964057","passport_expiry_date" : "2023-07-30 14:39:59.964057","ipaddress" : "142.195.1.154" , "mobile" : "9819415434"}
{"fname" : "Wayne","lname" : "iller","email" : "Wayne_iller@bnz.co.nz","principal" : "Wayne@EXAMPLE.COM","passport_make_date" : "2011-03-30 14:40:00.973376","passport_expiry_date" : "2021-03-30 14:40:00.973376","ipaddress" : "81.187.181.223" , "mobile" : "9828826164"}
{"fname" : "Christian","lname" : "weign","email" : "Christian_weign@tcs.com","principal" : "Christian@EXAMPLE.COM","passport_make_date" : "2013-02-28 14:40:01.982722","passport_expiry_date" : "2023-02-28 14:40:01.982722","ipaddress" : "158.169.175.39" , "mobile" : "9870023860"}
{"fname" : "Daniel","lname" : "weign","email" : "Daniel_weign#gmail.com","principal" : "Daniel@EXAMPLE.COM","passport_make_date" : "2016-07-29 14:40:02.992117","passport_expiry_date" : "2026-07-29 14:40:02.992117","ipaddress" : "203.81.64.23" , "mobile" : "9804581156"}

# ROUND 2: To Update the lname
{"fname" : "Thomas","lname" : "dagarin","email" : "Thomas_dagarin@@gmail.com","principal" : "Thomas@EXAMPLE.COM","passport_make_date" : "2013-10-29 14:40:05.003005","passport_expiry_date" : "2023-10-29 14:40:05.003005","ipaddress" : "108.125.242.158" , "mobile" : "9823308381"}
{"fname" : "Russell","lname" : "Wright","email" : "Russell_Wright@nbc.com","principal" : "Russell@EXAMPLE.COM","passport_make_date" : "2012-07-30 14:40:06.011988","passport_expiry_date" : "2022-07-30 14:40:06.011988","ipaddress" : "64.169.155.253" , "mobile" : "9848316824"}
{"fname" : "Kyle","lname" : "kumar","email" : "Kyle_kumar@hotmail.com","principal" : "Kyle@EXAMPLE.COM","passport_make_date" : "2012-04-30 14:40:07.021665","passport_expiry_date" : "2022-04-30 14:40:07.021665","ipaddress" : "198.196.75.63" , "mobile" : "9837699819"}

INFO: Use below Command to Start the Consumer !!

   $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server apache-spark.hadoop.com:9092 --topic mytopic_clean
```

Console 2: Follow the instructions displayed on Console 1.

  - Start the Kafka Producer using - $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list apache-spark.hadoop.com:9092 --topic mytopic
  - Copy Paste the sample records shown above.

```
[root@apache-spark ~]$  $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list apache-spark.hadoop.com:9092 --topic mytopic
>{"fname" : "Thomas","lname" : "dagarin","email" : "Thomas_dagarin@@gmail.com","principal" : "Thomas@EXAMPLE.COM","passport_make_date" : "2013-10-29 14:40:05.003005","passport_expiry_date" : "2023-10-29 14:40:05.003005","ipaddress" : "108.125.242.158" , "mobile" : "9823308381"}
{"fname" : "Russell","lname" : "Wright","email" : "Russell_Wright@nbc.com","principal" : "Russell@EXAMPLE.COM","passport_make_date" : "2012-07-30 14:40:06.011988","passport_expiry_date" : "2022-07-30 14:40:06.011988","ipaddress" : "64.169.155.253" , "mobile" : "9848316824"}
{"fname" : "Kyle","lname" : "kumar","email" : "Kyle_kumar@hotmail.com","principal" : "Kyle@EXAMPLE.COM","passport_make_date" : "2012-04-30 14:40:07.021665","passport_expiry_date" : "2022-04-30 14:40:07.021665","ipaddress" : "198.196.75.63" , "mobile" : "9837699819"}
>>>
>
>{"fname" : "Thomas","lname" : "dagarin","email" : "Thomas_dagarin@@gmail.com","principal" : "Thomas@EXAMPLE.COM","passport_make_date" : "2013-10-29 14:40:05.003005","passport_expiry_date" : "2023-10-29 14:40:05.003005","ipaddress" : "108.125.242.158" , "mobile" : "9823308381"}
{"fname" : "Russell","lname" : "Wright","email" : "Russell_Wright@nbc.com","principal" : "Russell@EXAMPLE.COM","passport_make_date" : "2012-07-30 14:40:06.011988","passport_expiry_date" : "2022-07-30 14:40:06.011988","ipaddress" : "64.169.155.253" , "mobile" : "9848316824"}
>>{"fname" : "Kyle","lname" : "kumar","email" : "Kyle_kumar@hotmail.com","principal" : "Kyle@EXAMPLE.COM","passport_make_date" : "2012-04-30 14:40:07.021665","passport_expiry_date" : "2022-04-30 14:40:07.021665","ipaddress" : "198.196.75.63" , "mobile" : "9837699819"}
>

```
Console 3: Follow the Instructions displayed on Console 1.
  
  - Start the Kafka Consumer using - $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server apache-spark.hadoop.com:9092 --topic mytopic_clean
  - Expect the streamed output having valid Email Addresses.

```
[root@apache-spark ~]$ $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server apache-spark.hadoop.com:9092 --topic mytopic_clean
{"is_email_valid":true,"fname":"Russell","lname":"Wright","email":"Russell_Wright@nbc.com","mobile":9848316824,"passport_expiry_year":2022,"email_check":"pass"}
{"is_email_valid":true,"fname":"Kyle","lname":"kumar","email":"Kyle_kumar@hotmail.com","mobile":9837699819,"passport_expiry_year":2022,"email_check":"pass"}
{"is_email_valid":true,"fname":"Russell","lname":"Wright","email":"Russell_Wright@nbc.com","mobile":9848316824,"passport_expiry_year":2022,"email_check":"pass"}
{"is_email_valid":true,"fname":"Kyle","lname":"kumar","email":"Kyle_kumar@hotmail.com","mobile":9837699819,"passport_expiry_year":2022,"email_check":"pass"}
```

## Contact
nrsh13@gmail.com
