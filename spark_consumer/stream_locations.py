import sys
import json
import redis
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext

"""
def raw_to_json(raw):
    data_list = raw.split(',')
    data = {"id": data_list[0],
            "date": data_list[1],
            "lat": data_list[2],
            "lng": data_list[3]}
    return json.dumps(data)
"""

def saveToRedis(iters):
    r = redis.StrictRedis(host="localhost", port=6379)
    for record in iters:
        data = record[1].split(',')
        key = record[0]
        value = str(data[2]) + ',' + str(data[3])
        r.set(key, value)
        r.lpush("last10", value)
        r.ltrim("last10", 0, 9)
        print("HERE!!!")
        print(key)
        print(value)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: stream_locations.py <zk> <topic>", file=sys.stderr)
        exit(1)

    # SparkContext - represents entry point to Spark cluster
    sc = SparkContext(appName="LocationStreamConsumer")

    # batchDuration of 1 - time interval in which data will be divided into
    # batches
    ssc = StreamingContext(sc, 1)

    zkQuorum = sys.argv[1]
    topic = sys.argv[2]

    # createStream returns a DStream object
    # DStream is a continuous sequence of RDDs
    # RDDs are the basic abstraction in Spark - these are datasets that can
    # be operated on in parallel
    kvs = KafkaUtils.createStream(ssc,
                                  zkQuorum,
                                  "spark-streaming-consumer",
                                  {topic: 1})
    
    # Represented as a tuple of (key, value)
    # lines = kvs.map(lambda x: x[1])
    # lines.pprint()
    # lines.foreachRDD(lambda rdd: rdd.foreachPartition(saveToRedis))
    kvs.foreachRDD(lambda rdd: rdd.foreachPartition(saveToRedis))

    # TODO: Store in redis - use tweet ID as the key
    # TODO: http://spark.apache.org/docs/latest/streaming-programming-guide.html#output-operations-on-dstreams

    ssc.start()
    ssc.awaitTermination()