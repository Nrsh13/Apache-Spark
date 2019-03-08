# Spark Streaming Examples

## Purpose
To Process Input Streams from Sources like Socket, Directory, Kafka & Flume and send to HBase using Spark Streaming.

## Prerequisites
```
happybase Python Module
  sudo pip install happybase
  or 
  sudo $ANACONDA_HOME/bin/python -m pip install happybase
  
Up and Running Hadoop Cluster with Apache Spark > 2.x

If you use Kafka as Source or Sink -
  Up and Running Kafka Cluster.
  Jars: spark-streaming-kafka-0.8-assembly_<scalaVersion>-<SparkVersion>.jar
```

## Description
Script will display all instructions like:
- How to start the Source say Kafka so that Spark can read it as a Stream.
- Sample data to be used as an input to the Source.
- Where to Check the Ouput and what command to use.
  
## Usage
```
[root@apache-spark ~]$ spark2-submit Socket-Dir-Kafka-Flume-Streaming-to-HBase.py help

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

          # For Socket and Directory Streaming:

          Usage: spark-submit Socket-Dir-Kafka-Flume-Streaming-to-HBase.py <service>

          # For Kafka and Flume Streaming:

          Usage: spark-submit --jars spark-streaming-<service>-assembly_<scalaVersion>-<SparkVersion>.jar Socket-Dir-Kafka-Flume-Streaming-to-HBase.py <service>

          Where <service> is:

                socket      : To Stream Data from a Socket
                directory   : To Stream Data from a HDFS Directory

                kafka       : To Stream Data from a Kafka Broker

                            Jar Needed: spark-streaming-kafka-0.8-assembly_<scalaVersion>-<SparkVersion>.jar

                            Download Link:
                                #1. Go to the corresponding Spark version website 'https://spark.apache.org/docs/2.3.0/streaming-kafka-0-8-integration.html'
                                Go to End of the 'Deploying Section' and click on 'Maven repository' ==> This will redirect to the correct jar download link.

                                OR

                                #2. Go to 'http://central.maven.org/maven2/org/apache/spark' ==> Browse to spark-streaming-kafka-0-8-assembly_<scalaVersion>/<sparkVersion>
                                #3. For CDH Cluster: /opt/cloudera/parcels/SPARK2/lib/spark2/kafka-0.9/spark-streaming-kafka-0-8_2.11-2.2.0.cloudera2.jar

                flume       : To Stream Data from Flume

                            Jar Needed: spark-streaming-flume-assembly_<scalaVersion>-<SparkVersion>.jar

                            Download Link:
                                #1. Go to the corresponding Spark version website 'https://spark.apache.org/docs/2.3.0/streaming-kafka-0-8-integration.html'
                                Go to End of the 'Deploying Section' and click on 'Maven repository' ==> This will redirect to the correct jar download link.

                                OR

                                #2. Go to 'http://central.maven.org/maven2/org/apache/spark' ==> Browse to spark-streaming-flume-assembly_<scalaVersion>/<sparkVersion>

                            Jar needed in Flume's lib Folder:
                                        spark-streaming-flume-sink_<scalaVersion>-<SparkVersion>.jar
                                        commons-lang3-3.5.jar
                                        scala-library-<scalaVersion>.jar

                            Download Link: https://spark.apache.org/docs/2.3.0/streaming-flume-integration.html#configuring-flume-1
```

## Example
Console 1: Run the main Script and go through the Instructions provided.
```
[root@apache-spark ~]$  spark2-submit --jars /opt/cloudera/parcels/SPARK2/lib/spark2/kafka-0.9/spark-streaming-kafka-0-8_2.11-2.2.0.cloudera2.jar Socket-Dir-Kafka-Flume-Streaming-to-HBase.py kafka

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

INFO: Setting up Spark Streaming Environment

INFO: Dropping/Recreating HBase Table 'sparkStreaming' with 'info' Column Family

INFO: Use below Command to Start the Producer

        $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list apache-spark.hadoop.com:9092 --topic mytopic

INFO: Use below sample Data(For- Socket/Kafka) or DataFile(For- Directory/Flume) one by one as Input

       ALERT: *** Make Sure you use the same Format ***

# ROUND 1: To Add some records
naresh,kumar,22
ravi,singh,33,hisar
akash,singh,45
bhanu,pratap,55

# ROUND 2: To Update the lname
naresh,KUMAR,22
akash,SINGH,45
bhanu,PRATAP,55

INFO: Streaming Started

        You can Press CTRL+C anytime to Stop Streaming

INFO: Start Pasting/Putting Sample Data in the Source

INFO: Check Produced messages in hbase Table using below Command

       echo "scan 'sparkStreaming'" | hbase shell

```

Console 2: Follow the instructions displayed on Console 1.

  - Start the Kafka Producer using - $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list apache-spark.hadoop.com:9092 --topic mytopic
  - Copy Paste the sample records shown above.

```
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list apache-spark.hadoop.com:9092 --topic mytopic
>naresh,kumar,22
ravi,singh,33,hisar
akash,singh,45
bhanu,pratap,55
>>>>naresh,KUMAR,22
akash,SINGH,45
bhanu,PRATAP,55
>>>>^C

```
Console 3: Follow the Instructions displayed on Console 1.
  
  - Check HBase Table - echo "scan 'sparkStreaming'" | hbase shell
  - Expect the streamed output having lname in Capital Letters.

```
echo "scan 'sparkStreaming'" | hbase shell
2019-03-08 15:46:54,074 INFO  [main] Configuration.deprecation: hadoop.native.lib is deprecated. Instead, use io.native.lib.available
HBase Shell; enter 'help<RETURN>' for list of supported commands.
Type "exit<RETURN>" to leave the HBase Shell
Version 1.2.0-cdh5.13.3, rUnknown, Sat Mar 17 04:43:46 PDT 2018

scan 'sparkStreaming'
ROW                                                          COLUMN+CELL
 akash-45                                                    column=info:age, timestamp=1552013210441, value=45
 akash-45                                                    column=info:fname, timestamp=1552013210441, value=akash
 akash-45                                                    column=info:lname, timestamp=1552013210441, value=SINGH
 bhanu-55                                                    column=info:age, timestamp=1552013210446, value=55
 bhanu-55                                                    column=info:fname, timestamp=1552013210446, value=bhanu
 bhanu-55                                                    column=info:lname, timestamp=1552013210446, value=PRATAP
 naresh-22                                                   column=info:age, timestamp=1552013210433, value=22
 naresh-22                                                   column=info:fname, timestamp=1552013210433, value=naresh
 naresh-22                                                   column=info:lname, timestamp=1552013210433, value=KUMAR
3 row(s) in 0.2330 seconds
```

## Contact
nrsh13@gmail.com
