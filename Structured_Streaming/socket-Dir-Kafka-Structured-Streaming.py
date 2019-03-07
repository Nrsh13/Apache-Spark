# Import SparkSQL Modules
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Other Python Modules
import os, sys, subprocess, json
import datetime, time
import socket, signal


# To Print the script Usage
def usage():
          print """
          # For Socket and Directory Streaming:
                Usage: spark-submit %s <input-source-type> <sink-type>

          # For Kafka Streaming:
                Usage: spark-submit --jars spark-sql-kafka-0-10_<scalaVersion>-<sparkVersion>.jar,kafka-clients-2.0.0.jar %s <input-source-type> <sink-type>

          Where
                input-source-type: socket or directory or kafka
                sink-type: console or file or kafka

                Jar Needed: spark-sql-kafka-0-10_<scalaVersion>-<SparkVersion>.jar and $KAFKA_HOME/libs/kafka-clients-2.0.0.jar
                Download Link:
                        #1. wget http://central.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.3.1/spark-sql-kafka-0-10_2.11-2.3.1.jar
                        #2. kafka-clients-2.0.0.jar can be found at $KAFKA_HOME/libs
                            \n""" %(sys.argv[0].split('/')[-1],sys.argv[0].split('/')[-1])
          sys.exit()


# To run any Shell Command
def runShell(args_list):
        '''
        Purpose: To Run any Shell Command
        '''
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        proc.communicate()
        return proc.returncode


# To Display the Output Location
def output_location(source,sink):
        if sink == 'file':
            print "INFO: Use below Command to Check the Output in HDFS !!"
            print "\n   hdfs dfs -ls /tmp/streamingOutput_"+source+"_"+sink+"_sink\n"
        elif sink == 'kafka':
            print "INFO: Use below Command to Start the Consumer !!"
            print "\n   $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server %s:%s --topic mytopic_clean\n" %(kafkaBrokerServer,kafkaBrokerPort)
        elif sink == 'console':
            print "INFO: Output will be Displayed on the Console !!\n"


# To Display Some Sample Data for Input
def sampleData():
        print "\nINFO: Use below sample Data(For- Socket/Kafka) or DataFile(For- Directory/Flume) one by one as Input"
        print "\n       ALERT: *** Make Sure you use the same Format ***"
        print """
# ROUND 1: To Add some records
{"fname" : "Billy","lname" : "Clark","email" : "Billy_Clark@@yahoo.com","principal" : "Billy@EXAMPLE.COM","passport_make_date" : "2013-07-30 14:39:59.964057","passport_expiry_date" : "2023-07-30 14:39:59.964057","ipaddress" : "142.195.1.154" , "mobile" : "9819415434"}
{"fname" : "Wayne","lname" : "iller","email" : "Wayne_iller@bnz.co.nz","principal" : "Wayne@EXAMPLE.COM","passport_make_date" : "2011-03-30 14:40:00.973376","passport_expiry_date" : "2021-03-30 14:40:00.973376","ipaddress" : "81.187.181.223" , "mobile" : "9828826164"}
{"fname" : "Christian","lname" : "weign","email" : "Christian_weign@tcs.com","principal" : "Christian@EXAMPLE.COM","passport_make_date" : "2013-02-28 14:40:01.982722","passport_expiry_date" : "2023-02-28 14:40:01.982722","ipaddress" : "158.169.175.39" , "mobile" : "9870023860"}
{"fname" : "Daniel","lname" : "weign","email" : "Daniel_weign#gmail.com","principal" : "Daniel@EXAMPLE.COM","passport_make_date" : "2016-07-29 14:40:02.992117","passport_expiry_date" : "2026-07-29 14:40:02.992117","ipaddress" : "203.81.64.23" , "mobile" : "9804581156"}

# ROUND 2: To Update the lname
{"fname" : "Thomas","lname" : "dagarin","email" : "Thomas_dagarin@@gmail.com","principal" : "Thomas@EXAMPLE.COM","passport_make_date" : "2013-10-29 14:40:05.003005","passport_expiry_date" : "2023-10-29 14:40:05.003005","ipaddress" : "108.125.242.158" , "mobile" : "9823308381"}
{"fname" : "Russell","lname" : "Wright","email" : "Russell_Wright@nbc.com","principal" : "Russell@EXAMPLE.COM","passport_make_date" : "2012-07-30 14:40:06.011988","passport_expiry_date" : "2022-07-30 14:40:06.011988","ipaddress" : "64.169.155.253" , "mobile" : "9848316824"}
{"fname" : "Kyle","lname" : "kumar","email" : "Kyle_kumar@hotmail.com","principal" : "Kyle@EXAMPLE.COM","passport_make_date" : "2012-04-30 14:40:07.021665","passport_expiry_date" : "2022-04-30 14:40:07.021665","ipaddress" : "198.196.75.63" , "mobile" : "9837699819"}
"""


# Validating Email address if its not like nrsh13@gmail.com
def validate_email(email_address):
        try:
                import re
                match = re.match('^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$', email_address.lower())
                if match == None:
                        status = False
                else:
                        status = True
                return status
        except Exception,e:
                print ("Failed to Validate the Email Address !!")
                print ("ERROR: " , e)


# To Process the Incoming Stream
def process_streamed_df(recordDF,sink,kafkaBroker='localhost:9092'):

        #print "Does this DF has Streaming Data: " , recordDF.isStreaming, "\n"

        # Adding the EVENT Time
        recordDF = recordDF.withColumn("event_time", F.current_timestamp().cast(T.TimestampType()))

        # Just Renaming a Column
        recordDF = recordDF.select(
                      recordDF.fname,
                      recordDF.lname,
                      recordDF.email,
                      recordDF.passport_make_date,
                      recordDF.passport_expiry_date,
                      recordDF.ipaddress,
                      recordDF.mobile,
                      )\
                      .withColumnRenamed(
                        "principal",
                        "realm"
                      )

        # Add new column having Email validity Status
        recordDF = recordDF.select("*" , validate_email_address(recordDF.email).alias("is_email_valid"))

        # Change the Data Type of passport_make_year and passport_expiry_year from String to TimestampType()
        recordDF = recordDF.withColumn("passport_make_date", recordDF.passport_make_date.cast(T.TimestampType()))\
                .withColumn("passport_expiry_date", recordDF.passport_expiry_date.cast(T.TimestampType()))\
                .withColumn("mobile", recordDF.mobile.cast(T.LongType()))

        # Generate a new column showing the year for passport make and expire
        recordDF = recordDF.withColumn("passport_make_year", F.year(recordDF.passport_make_date).cast(T.IntegerType()))\
                .withColumn("passport_expiry_year", F.year(recordDF.passport_expiry_date).cast(T.IntegerType()))


        # Choose Required columns:
        recordDF = recordDF.select(
                      recordDF.fname,
                      recordDF.lname,
                      recordDF.email,
                      recordDF.mobile,
                      recordDF.passport_expiry_year,
                      recordDF.is_email_valid,
                      )

        """
        WE CAN RUN MULTIPLE QUERIES WITH DIFFERNT SETTINGS/MODES/TRIGGERS ETC.
                Trigger Options: Default(Nothing needed), .trigger(processingTime='2 seconds') ,.trigger(once=True), .trigger(continuous='1 second')
                Output Mode Options: .outputMode("complete"), .outputMode("append") [Default as well] and .outputMode("update")
        """

        # Apply Join Operation
        #staticDF = myspark.createDataFrame([["true","pass"],["false","fail"],],["is_email_valid","email_check"])
        staticDF = myspark.createDataFrame([["true","pass"],],["is_email_valid","email_check"])
        recordDF = recordDF.join(staticDF, "is_email_valid")

        recordDF.createOrReplaceTempView("mytable")

        ##### APPEND MODE: We can simply select all Columns. No Aggregation Needed
        recordDF = myspark.sql("select * from mytable")

        # Choose required Sink
        if sink == 'console':
                # Console Sink
                append_mode_query = recordDF.writeStream\
                      .outputMode("append")\
                      .format("console")\
                      .option("truncate", "false")\
                      .option("numRows", 5)\
                      .trigger(processingTime='5 seconds')\
                      .start()

        elif sink == 'file':
                # File Sink
                append_mode_query = recordDF.writeStream\
                      .outputMode("append")\
                      .format("parquet")\
                      .option("path", "/tmp/streamingOutput_"+source+"_"+sink+"_sink")\
                      .option("checkpointLocation", "/tmp/"+source+"_"+sink+"_checkpoint") \
                      .trigger(processingTime='5 seconds')\
                      .start()

        elif sink == 'kafka':
                # Kafka Sink
                append_mode_query = recordDF.select(F.to_json(F.struct("*")).alias("value")).writeStream\
                    .format("kafka")\
                    .option("kafka.bootstrap.servers", kafkaBroker)\
                    .option("topic", "mytopic_clean")\
                    .option("checkpointLocation", "/tmp/"+source+"_"+sink+"_checkpoint") \
                    .trigger(processingTime='5 seconds')\
                    .start()

        # Instead of using .awaitTermination() in the query, use below when you have Multiple Queries.
        myspark.streams.awaitAnyTermination()



# Main Function
if __name__ == '__main__':

        import socket
        hostname = socket.gethostname()

        print """
        ALERT: This Script assumes All Services are running on same Machine %s !!
        if NOT, Update the required Details in Main() section of the Script.""" %hostname

        # Check Number of Arguments
        if len(sys.argv) == 2 and sys.argv[1].lower() in ['h', 'help', 'usage']:
                usage()

        if len(sys.argv) != 3 or sys.argv[1].lower() not in ['socket','directory','kafka'] or sys.argv[2].lower() not in ['console','file','kafka']:
                print "\n       ERROR: Wrong Name/Number of Arguments !!"
                usage()

        # Source and Sink INFO
        source = sys.argv[1].lower()
        sink = sys.argv[2].lower()

        # Remove Older CheckPoint path so that same name input file can be picked up.
        cmd = "hadoop fs -rm -r /tmp/"+source+"_"+sink+"_checkpoint /tmp/streamingOutput_"+source+"_"+sink+"_sink &>/dev/null"
        os.system(cmd)

        # Socket Server Details
        socketServer = hostname
        socketPort = 9999

        # Kafka Details
        kafkaBrokerServer = hostname
        kafkaBrokerPort = 9092
        zookeeperServer = hostname
        zookeeperPort = 2185

        kafkaBroker = kafkaBrokerServer+":"+str(kafkaBrokerPort)

        print "\nINFO: Setting up Spark Streaming Environment"
        myspark = SparkSession.builder.master("yarn").appName("SPARK_Streaming").enableHiveSupport().getOrCreate()

        schema = T.StructType([T.StructField("fname", T.StringType(), True),\
                         T.StructField("lname", T.StringType(), True),\
                         T.StructField("email", T.StringType(), True),\
                         T.StructField("principal", T.StringType(), True),\
                         T.StructField("passport_make_date", T.StringType(), True),\
                         T.StructField("passport_expiry_date", T.StringType(), True),\
                         T.StructField("ipaddress", T.StringType(), True),\
                         T.StructField("mobile", T.StringType(), True)])


        # Register a UDF:
        validate_email_address = F.udf(validate_email, T.BooleanType())

        # Socket Streaming
        if source == 'socket':

                print "\nINFO: Use below Command to Start Socket Server ASAP. Waiting for 10 Seconds"
                print "\n       nc -lk %s 9999" %hostname
                import time
                time.sleep(10)

                sampleData()

                output_location(source,sink)

                socketStream = myspark\
                        .readStream\
                        .format('socket')\
                        .option('host', socketServer)\
                        .option('port', socketPort)\
                        .load()

                #socketStream.printSchema()

                # Filter EMPTY Records [col.isNotNull() does not work here]
                recordDF = socketStream.select(socketStream.value).where(socketStream.value != '')

                # If the Input coming is JSON message - eq: {"fname":"naresh","lname":"jangra","age":30,"height":170.5,"dated":"2013-10-12","timing":"2013-10-12 12:35:50"}
                # the recordDF.value column will have JSON Record with type STRING. We need to get all fields from the STRING JSON Value and apply Schema.
                recordDF = recordDF.withColumn("value", F.from_json("value", schema)).select(F.col('value.*'))

                # If the Input coming is comma separated records - eq: "naresh ,jangra,30,170.5,2013-10-12,2013-10-12 12:35:50"
                '''
                split_col = F.split(recordDF['value'], ',')
                recordDF = recordDF.withColumn('fname', split_col.getItem(0))
                recordDF = recordDF.withColumn('lname', split_col.getItem(1))
                recordDF = recordDF.withColumn('age', split_col.getItem(2))
                recordDF = recordDF.select(recordDF.fname,recordDF.lname,recordDF.age)
                '''

                process_streamed_df(recordDF,sink,kafkaBroker)

        # Directory Streaming
        elif source == 'directory':

            if runShell(['hdfs', 'dfs', '-test', '-d', '/tmp/Dir_spark_Streaming_Test']) != 0:
                cmd = "hdfs dfs -mkdir /tmp/Dir_spark_Streaming_Test"
                os.system(cmd)
            else:
                cmd = "hdfs dfs -rm -r /tmp/Dir_spark_Streaming_Test/* &>/dev/null"
                os.system(cmd)

            print "\nINFO: Use below Command to put Sample Data File in HDFS"
            print "\n   hdfs dfs -put sampledata.json /tmp/Dir_spark_Streaming_Test"

            sampleData()

            output_location(source,sink)

            dirStream = myspark.readStream\
                               .option("header", "false")\
                               .option("maxFilesPerTrigger", 1)\
                               .schema(schema)\
                               .json("/tmp/Dir_spark_Streaming_Test")

            process_streamed_df(dirStream,sink,kafkaBroker)

        # Kafka Streaming
        elif source == 'kafka':

                print "\nINFO: Use below Command to Start the Producer"
                print "\n       $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list %s:%s --topic mytopic" %(kafkaBrokerServer,kafkaBrokerPort)

                sampleData()

                output_location(source,sink)

                kafkaStream = myspark\
                        .readStream\
                        .format('kafka')\
                        .option('kafka.bootstrap.servers', kafkaBroker)\
                        .option('subscribe', 'mytopic')\
                        .load()\
                        .select(F.from_json(F.col("value").cast("string"), schema).alias("value"))

                # Kafka stream always produces following fields: value, offset, partition, key, timestamp, timestampType, topic
                # Filter EMPTY Records
                recordDF = kafkaStream.select("value.*").where(kafkaStream.value.isNotNull())

                process_streamed_df(recordDF,sink,kafkaBroker)
