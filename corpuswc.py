import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import Row

spark = SparkSession.builder.appName("split").getOrCreate()
delim='nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword '
infile = '/tmp/xaa'
if len(sys.argv) == 2:
  infile = sys.argv[1]
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", delim)


row = Row("id", "text")
textdf = sc.textFile(infile).map(lambda x: x.replace('\n', '')).map(lambda x: x.split(' ', 1)).toDF(row)
textdf = textdf.withColumn('id', f.regexp_replace(textdf.id, 'nferdoccount_', ''))
textdf = textdf.withColumn('wordcount', f.size(f.split(f.col('text'), ' ')))
for i in textdf.take(3):
  print(i.id, i.wordcount)
  print(" --")

