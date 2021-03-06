#from auxApp import my_text_f
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

my_list = ["bla bla",
           "blo blu ble",
           "bli bla ble blu",
           "blo blu bla ble bli",
           "arg bly blo blu bla ble bli",
           "erg bly blo blu bla ble bli",
           "erg bly blo blu bla ble bli zapp" ]

def count_words(x, debug=True):
    sp = x.split()
    if debug:
        print("\n\n"+"->"+sp[0], len(x), "\n")
    return sp.map(lambda word: (word, 1))

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCountRM")
ssc = StreamingContext(sc, 3)
kafkaStream = KafkaUtils.createStream(ssc,
                                      'broker.kafka.l4lb.thisdcos.directory:9092',
                                      'raw-event-streaming-consumer',
                                      {'topic1': 1})
#
# parsed = kafkaStream.map(lambda v: v.strip())
# parsed = sc.parallelize(my_list)
# counts = parsed.map(count_words)

lines = kafkaStream.map(lambda x: x[1])
counts = lines.flatMap(count_words).reduceByKey(lambda a, b: a + b)

# print("\n\n\n\n XXXX \n\n\n\n")
# reduction = counts.reduce(lambda a, b: a + b)

print("\n ===================================== \n")
# print("\n\nFINAL RESULT\n" + str(reduction))
counts.pprint()
print("_____________________________________\n\n\n")

ssc.start()
