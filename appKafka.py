from auxApp import my_text_f
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCountRM")
ssc = StreamingContext(sc, 3)
kafkaStream = KafkaUtils.createStream(ssc,
                                      'master.mesos:2181/dcos-service-kafka',
                                      'spark-streaming',
                                      {'topic1':1})

parsed = kafkaStream.map(lambda v: v.strip())
counts = parsed.map(my_text_f)
print("\n\n\n\n XXXX \n\n\n\n")
reduction = counts.reduce(lambda a, b: a + b)


print("\n\nFINAL RESULT\n" + str(reduction))
print("\n\n\n")
