from __future__ import print_function
from auxApp import extract_words as my_f

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

if __name__ == "__main__":
    from configs.kafkaconfigs import *

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 2)

    fromOffsets = {TopicAndPartition(topic, 0): int(0)}
    zkQuorum, topic = br, topic
    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                        {"metadata.broker.list": zkQuorum},
                                        fromOffsets=fromOffsets
                                        )

    lines = kvs.map(lambda x: x[1])
    # counts = lines.flatMap(lambda line: line.split(" "))\
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    counts = lines.flatMap(my_f).reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
