import sys
import re
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import Row
from operator import add
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, StringType
from pyspark.sql.functions import lower, col


spark = SparkSession.builder.appName("split").getOrCreate()


def processRegex(txt):
    #return txt
    regex = r'[{}|$\[\]\(\)%;,]'
    txt = re.sub(regex, ' \g<0> ', txt.lower())
    txt = re.sub(r'[^\x00-\x7f]',r'', txt)
    txt = re.sub(r" [-_] ", " _ ", txt)
    txt = txt.replace(".", " .")
    txt = re.sub(r" [^{}\[\]() %&\-a-zA-Z0-9:_+.,]"," \g<0>",txt)
    return txt.split(' ')

normalzerUdf = udf(processRegex, StringType())

def getOffset():
   #find and return the shard offet
   return 0
documentOffset = getOffset()

#nference Document separator 
nferDocSeperator='nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword '
infile = '/tmp/xaa'
infile = '/tmp/smpl.txt'
infile = '/tmp/r.txt'
if len(sys.argv) == 2:
  infile = sys.argv[1]
sc = spark.sparkContext
#sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", nferDocSeperator)

row = Row("text", "id")
rawRdd = sc.textFile(infile).filter(lambda x: len(x) > 1 and 'nferstopword' not in x and 'nferdoccount_' not in x)
#normRdd = rawRddWithIdx.map(normalzerUdf)
normRdd = rawRdd.map(processRegex)
rawRddWithIdx = normRdd.zipWithIndex()

#exit(0)
print("normalized with index")
#for i in rawRddWithIdx.take(10):
  #print(i)
print("normalized count:", normRdd.count())
dfflat = rawRddWithIdx.flatMap(lambda x: [(y, x[1]) for y in x[0]])
for i in dfflat.take(40):
  print(i)
#exit(0)
termfrq = dfflat.countByKey()
docfrq = dfflat.distinct().countByKey()

tdf = sc.parallelize([(k,)+(v,) for k,v in termfrq.items()]).toDF(['word','tf'])
ddf = sc.parallelize([(k,)+(v,) for k,v in docfrq.items()]).toDF(['word','df'])

ddf.show(30)
tdf.show(30)

cols = ['word', 'tf', 'df']

final = ddf.join(tdf, "word")
try:
  final.show(10)
except:
  pass
outfile = infile + "-vocab.csv"
print("Saving output in " + outfile)
final.coalesce(1)
final.write.format("csv").mode("overwrite").options(header="false",sep="\t").save(path=outfile)
