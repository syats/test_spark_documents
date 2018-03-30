from __future__ import print_function
from auxApp import count_words

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

if __name__ == "__main__":

    zk = "master.mesos:2181/dcos-service-kafka"
    br = "broker.kafka.l4lb.thisdcos.directory:9092"
    topic = "topic1"

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    fromOffsets = {TopicAndPartition(topic, 0): int(0)}

    zkQuorum, topic = br, topic
    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                        {"metadata.broker.list": zkQuorum},
                                        # fromOffsets=fromOffsets
                                        )
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
    #
    # lines = kvs.map(lambda x: x[1])
    # counts = lines.map(count_words).reduceByKey(lambda a, b: a+b)
    # counts.pprint()
    #
    # ssc.start()
    # ssc.awaitTermination()