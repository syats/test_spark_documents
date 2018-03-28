from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer
from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.ml.clustering import LDA, LDAModel, BisectingKMeans

import os
from auxApp import my_text_f

app_name = "Visapp1"
if os.environ.get('SPARK_MASTER_URL') is not None:
    conf = SparkConf().setAppName(app_name).setMaster(os.environ["SPARK_MASTER_URL"])
else:
    conf = SparkConf().setAppName(app_name)
sc = SparkContext(conf=conf)

folderLocation = 'test_data/'

distData = sc.wholeTextFiles(folderLocation)
coincidences = distData.map(my_text_f)
print("\n\n\n\n XXXX \n\n\n\n")
reduction = coincidences.reduce(lambda a, b: a + b)


print("\n\nFINAL RESULT\n" + str(reduction))
print("\n\n\n")