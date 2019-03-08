# Python Modules
import happybase
import os, sys, subprocess, json
import datetime, time
import socket

# SparkSQL Modules
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types as T

# SparkStreaming Modules
from pyspark.streaming import StreamingContext


# To Print the script Usage
def usage():
          print """
          # For Socket and Directory Streaming:

          Usage: spark-submit %s <service>

          # For Kafka and Flume Streaming:

          Usage: spark-submit --jars spark-streaming-<service>-assembly_<scalaVersion>-<SparkVersion>.jar %s <service>

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

                            Download Link: https://spark.apache.org/docs/2.3.0/streaming-flume-integration.html#configuring-flume-1\n""" %(sys.argv[0].split('/')[-1],sys.argv[0].split('/')[-1])
          sys.exit()


# To run any Shell Command
def runShell(args_list):
        '''
        Purpose: To Run any Shell Command
        '''
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        proc.communicate()
        return proc.returncode


# To Display Some Sample Data for Input
def sampleData():
        print "\nINFO: Use below sample Data(For- Socket/Kafka) or DataFile(For- Directory/Flume) one by one as Input"
        print "\n       ALERT: *** Make Sure you use the same Format ***"
        print """
# ROUND 1: To Add some records
naresh,kumar,22
ravi,singh,33,hisar
akash,singh,45
bhanu,pratap,55

# ROUND 2: To Update the lname
naresh,KUMAR,22
akash,SINGH,45
bhanu,PRATAP,55"""


# To Process the Incoming Stream
def processRddPartitions(partition):
        '''
        Purpose: Process the Rdd Batch per Partition.
        '''

        '''
        As we are using foreachPartition: Below we are making the Connection Object which is per Executor.
        Else Error: PicklingError: Could not serialize object: ImportError: No module named cybin
        '''

        # To get the Type of the input RDD Elements
        # print type(partition)

        connection = happybase.Connection(hbaseThriftServer,hbaseThriftPort)
        table = connection.table('sparkStreaming')

        # Check for Empty Records
        '''
        Use of rdd.isEmpty() ==>

        When we use foreachPartiton: we can not run isEmpty() because our input is NOT an RDD here. It's records of type
                Unicode(when you send the stream directly)
                Tuple/List (when you send the stream after aplying map/filter function etc.

        Error: AttributeError: 'generator'/Iterator object has no attribute 'map'

        When we only use foreachRDD: Input will be an RDD and we can check if the RDD is empty or not like below.

        if not records.isEmpty():
                print "\nProcessing Each RDD"
                for record in rdd.collect():
                        ** Do Required Stuff **
        '''

        for record in partition:
                try:
                        fname = record[0]
                        lname = record[1]
                        age = record[2]

                        # Prepare Key,Value for HBase
                        key = fname+'-'+str(age)
                        value = {'info:fname' : fname, 'info:lname' : lname, 'info:age' : str(age)}

                        # Writting to HBase Table
                        table.put(key,value)

                except Exception, e:
                        print "Partition Processing Failed with %s" %e

        # Read the Data from HBase. Will be printed in Executor logs
        #for key, data in table.scan():
        #    print(key, data)

        # Do not close the connection inside the loop. Rest of the records will not be processed.
        # 'NoneType' object has no attribute 'sendall'
        connection.close()


# Main Function
if __name__ == '__main__':

        import socket
        hostname = socket.gethostname()

        print """
        ALERT: This Script assumes All Services are running on same Machine %s !!
        if NOT, Update the required Details in Main() section of the Script.""" %hostname

        # Check Number of Arguments
        if len(sys.argv) != 2 or sys.argv[1].lower() not in ['socket','directory','kafka','flume']:
                usage()

        if len(sys.argv) == 2 and sys.argv[1].lower() in ['h', 'help', 'usage']:
                usage()

        # HBase Details
        hbaseThriftServer = "pxlbig10"
        hbaseThriftPort = 9090

        # Socket Server Details
        socketServer = hostname
        socketPort = 9999

        # Kafka Details
        kafkaBrokerServer = hostname
        kafkaBrokerPort = 9092
        kafkaBroker = kafkaBrokerServer+":"+str(kafkaBrokerPort)

        zookeeperServer = hostname
        zookeeperPort = 2185
        zookeeper = zookeeperServer+":"+str(zookeeperPort)

        # Flume Details
        flumeAgentSinkServer = hostname
        flumeAgentSinkPort = 9999

        from pyspark.streaming import StreamingContext
        from pyspark.sql import SparkSession
        from pyspark import SparkConf

        #For Hortonworks:
        '''
        mylist = [('spark.yarn.dist.files','file:/usr/hdp/current/spark2-client/python/lib/pyspark.zip,file:/usr/hdp/current/spark2-client/python/lib/py4j-0.10.6-src.zip')]
        conf = SparkConf().setAll(mylist)
        conf.setExecutorEnv('PYTHONPATH','pyspark.zip:py4j-0.10.6-src.zip')
        myspark = SparkSession.builder.master("yarn").appName("Spark_Streaming").config(conf=conf).enableHiveSupport().getOrCreate()
        '''

        print "\nINFO: Setting up Spark Streaming Environment"
        myspark = SparkSession.builder.master("yarn").appName("SPARK_Streaming").enableHiveSupport().getOrCreate()
        ssc = StreamingContext(myspark.sparkContext,10)

        # HBase Setup
        '''
        Below we are dropping and recreating the HBase table.

        In foreachRDD: - HBase connection will be created in this section (DRIVER Section). The Processed data will finally come
        to Drive from all executors and get written to HBase using same 'Connection' Object. NOT GOOD for Big DataSets.

        In foreachPartition: HBase Connectoin should be created during RDD processing in the function. Which means we are creating Connection for
        each Executor. If we try passing this Connection from Driver Section to foreachPartition - We may fail with strange ERRORS like below as it is
        not possible to send a connection to worker node.

        Error: PicklingError: Could not serialize object: ImportError: No module named cybin
        Refer: https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
        '''

        print "\nINFO: Dropping/Recreating HBase Table 'sparkStreaming' with 'info' Column Family"
        import happybase
        try:
                connection = happybase.Connection(hbaseThriftServer,hbaseThriftPort)
                connection.disable_table('sparkStreaming')
                connection.delete_table('sparkStreaming')

        except Exception, e:
                print "\nWARN: Drop Table Failed. Table might not be available."

        families = { 'info': dict(max_versions=10)}
        connection.create_table('sparkStreaming', families=families)
        connection.close()

        # Socket Streaming
        if sys.argv[1].lower() == 'socket':

                print "\nINFO: Use below Command to Start Socket Server ASAP. Waiting for 10 Seconds"
                print "\n       nc -lk %s 9999" %hostname
                import time
                time.sleep(10)

                sampleData()

                socketStream = ssc.socketTextStream(socketServer,socketPort)
                #socketStream.pprint()
                '''
                naresh,kumar,22
                ravi,singh,33,hisar
                akash,singh,45
                bhanu,pratap,55
                '''
                input_stream = socketStream.filter(lambda x: x != None and len(x) > 0 )

        # Directory Streaming
        elif sys.argv[1].lower() == 'directory':

            if runShell(['hdfs', 'dfs', '-test', '-d', '/tmp/Dir_spark_Streaming_Test']) != 0:
                cmd = "hdfs dfs -mkdir /tmp/Dir_spark_Streaming_Test"
                os.system(cmd)
            else:
                cmd = "hdfs dfs -rm -r /tmp/Dir_spark_Streaming_Test/* &>/dev/null"
                os.system(cmd)

            print "\nINFO: Use below Command to put Sample Data File in HDFS"
            print "\n   hdfs dfs -put yourfile.csv /tmp/Dir_spark_Streaming_Test"

            sampleData()

            dirStream = ssc.textFileStream("/tmp/Dir_spark_Streaming_Test")
            input_stream = dirStream.filter(lambda x: x != None and len(x) > 0 )

        # Kafka Streaming
        elif sys.argv[1].lower() == 'kafka':

                print "\nINFO: Use below Command to Start the Producer"
                print "\n       $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list %s:%s --topic mytopic" %(kafkaBrokerServer,kafkaBrokerPort)

                sampleData()

                from pyspark.streaming.kafka import KafkaUtils

                # Receiver-based Approach
                '''
                This approach uses a Receiver to receive the data. The Receiver is implemented using the Kafka high-level consumer API. As with all
                receivers, the data received from Kafka through a Receiver is stored in Spark executors, and then jobs launched by Spark Streaming
                processes the data.
                '''
                #kstream = KafkaUtils.createStream(ssc, [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])
                #kstream = KafkaUtils.createStream(ssc, zookeeperServer+":"+str(zookeeperPort), "Streaming_CGroup" , {"test" : 1})

                # Direct Approach (No Receivers)
                '''
                Instead of using receivers to receive data, this approach periodically queries Kafka for the latest offsets in each topic+partition,
                and accordingly defines the offset ranges to process in each batch
                '''
                #kstream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
                kstream = KafkaUtils.createDirectStream(ssc, ["mytopic"] , {"metadata.broker.list": kafkaBrokerServer+":"+str(kafkaBrokerPort)})

                '''
                #kstream.pprint() # <Kafka topic, Kafka message>
                (None, u'naresh,kumar,22')
                (None, u'')
                (None, u'ravi,singh,33,hisar')
                '''

                input_stream = kstream.map(lambda x: x[1])
                '''
                input_stream.pprint()
                naresh,kumar,22

                ravi,singh,33,hisar
                '''

        # Flume Streaming
        elif sys.argv[1].lower() == 'flume':

                print "\nINFO: Make sure you have your Flume Agent spooling /tmp/Flume_spark_Streaming_Test is Up and Running."
                print "\n       flume-ng agent -c . -f flume_spark_stream.conf --name tier1 -Dflume.root.logger=INFO,console"
                print """\n     Content of flume_spark_stream.conf:

                ## Configuring Components
                tier1.sources=source1
                tier1.channels=channel1
                tier1.sinks=sink1

                ## Configuring Source
                tier1.sources.source1.type=spooldir
                tier1.sources.source1.spoolDir=/tmp/Flume_spark_Streaming_Test
                tier1.sources.source1.channels=channel1

                ## Configuring Channel
                tier1.channels.channel1.type=memory
                tier1.channels.channel1.capacity=10000
                tier1.channels.channel1.transactionCapacity=1000

                ## Configuring sink
                tier1.sinks.sink1.type=org.apache.spark.streaming.flume.sink.SparkSink
                tier1.sinks.sink1.channel=channel1
                tier1.sinks.sink1.hostname=myhostname # Update your Hostname here
                tier1.sinks.sink1.port=9999
                """

                sampleData()

                cmd = "rm -rf /tmp/Flume_spark_Streaming_Test/*"
                os.system(cmd)

                from pyspark.streaming.flume import FlumeUtils

                port = 9999
                addresses = {(hostname, port)}

                # Pull-based Approach using a Custom Sink
                '''
                Flume pushes data into the sink, and the data stays buffered.
                Spark Streaming uses a reliable Flume receiver(Jar-org.apache.spark.streaming.flume.sink.SparkSink) and transactions to pull
                data from the sink
                '''
                fstream = FlumeUtils.createPollingStream(ssc, addresses)
                #fstream.pprint() # ({}, u'naresh,kumar,22')

                input_stream = fstream.map(lambda x: x[1])


        # Processing the DStream
        '''
        This step will do the required processing/filtering on the main DStream and generate a Tuple or List or raw value(when we directly send
        input_stream without any map/filter).

        In case of any confusion after any map/filter, like what is the Type(list/Tuple/Raw) of DStream, just use print type(xyz) in the function
        where we are sending this DStream. This will print the type in any of the executor (but NOT on the CONSOLE). From there you get an idea about the
        type and process the records accordingly.
        '''

        # This sends records as Tuple - (u'naresh', u'kumar', u'21')
        # dStream = input_stream.map(lambda x: x.split(",")).map(lambda x: (x[0],x[1],int(x[2])))
        # This sends records as list - [u'naresh', u'kumar', u'21']
        dStream = input_stream.map(lambda x: x.split(","))

        # Ingore Empty and 4/more Columns records
        dStream = dStream.filter(lambda x: x != None and len(x) == 3 )

        # To see every DStream on the Console
        # dStream.pprint()

        print "\nINFO: Streaming Started "
        print "\n       You can Press CTRL+C anytime to Stop Streaming"

        print "\nINFO: Start Pasting/Putting Sample Data in the Source"
        print "\nINFO: Check Produced messages in hbase Table using below Command"
        print "\n       echo \"scan 'sparkStreaming'\" | hbase shell"
        print "\n"

        # Send to External System
        '''
        dstream.foreachRDD is a powerful primitive that allows data to be sent out to external systems

        Using foreachRDD, We can process each RDD, get all outputs to Driver and send to External System like HBase, Cassandra. But in heavy batches,
        Driver will get heavily Loaded.

        dStream.foreachRDD(lambda rdd: processRddPartitions(rdd,table))

        OR

        Using foreachPartition, We can process the complete Partition and send the output directly from Executor to External System.
        '''

        dStream.foreachRDD(lambda rdd: rdd.foreachPartition(processRddPartitions))

        ssc.start()
        ssc.awaitTermination()
        # ssc.awaitTerminationOrTimeout(60) # Stop the Application is Data Source is NOT Active
        # ssc.stop()
