import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: stream_locations.py <zk> <topic>", file=sys.stderr)
        exit(1)

    # SparkContext - represents entry point to Spark cluster
    sc = SparkContext(appName="LocationStreamConsumer")

    # batchDuration of 1 - time interval in which data will be divided into
    # batches
    ssc = StreamingContext(sc, 1)

    zkQuorum= sys.argv[1]
    topic = sys.argv[2]

    # TODO: Not sure what groupId does ("spark-streaming-consumer")
    # createStream returns a DStream object
    # DStream is a continuous sequence of RDDs
    # RDDs are the basic abstraction in Spark - these are datasets that can
    # be operated on in parallel
    kvs = KafkaUtils.createStream(ssc,
                                  zkQuorum,
                                  "spark-streaming-consumer",
                                  {topic: 1})
    
    # Represented as a tuple of (key, value)
    lines = kvs.map(lambda x: x[1])
    lines.pprint()

    """
    counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    """

    ssc.start()
    ssc.awaitTermination()