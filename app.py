from pyspark import SparkContext, SparkConf
import os
from auxApp import my_text_f



conf = SparkConf().setAppName("Visapp1").setMaster(os.environ["SPARK_MASTER_URL"])
sc = SparkContext(conf=conf)

folderLocation = 'test_data/'

distData = sc.wholeTextFiles(folderLocation)
coincidences = distData.map(my_text_f)
print("\n\n\n\n XXXX \n\n\n\n")
reduction = coincidences.reduce(lambda a, b: a + b)


print("\n\nFINAL RESULT\n" + str(len(reduction)))
print("\n\n\n")