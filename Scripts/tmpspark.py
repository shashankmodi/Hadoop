# export QD=/media/sf_Data/Hadoop/Scripts
# export QS=/media/sf_Data
# pyspark $QS/tmpspark.py >$QD/pyspark.log 2>$QD/pyspark2.log

from pyspark import SparkContext
import datetime
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import lit


def quiet_logs( sc ):
  print "Resetting the log level for Spark...",str(sc.version)
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def basicSum(nums):
  return nums.fold(0,(lambda x,y:x+y))

tnow = datetime.datetime.now()
print("Begin of Program...Spark : %s" % tnow)
  
sc =SparkContext(appName="Dataframe")
sqlc = SQLContext(sc)
quiet_logs( sc )

########### my pySpark code ###############
NameAge = Row('fname','lname','age') # build a Row subclass
data_rows = [
	NameAge('Udhay','Subramanian',40),
	NameAge('Sundar','Subramanian',35),
	NameAge('Sundar','Subramanian',35),
	NameAge('Mridhini','Udhay',8),
	NameAge('Srishti','Udhay',8),
	NameAge('Anu','Udhay',35),
	NameAge('Saraswathi','Palani',38)
]
data_rdd= sc.parallelize(data_rows)
df = sqlc.createDataFrame(data_rows)
df.registerTempTable("people")

print "data_rdd:",type(data_rdd)
print "df:",type(df)

print "RDD" , data_rdd.toDebugString()
print "Data Frame" , df.printSchema()
print df.show()
print "where:\t", df.where (df['age'] <30).show()
print "where eq:\t", df.where (df['fname'] ==lit('Udhay')).show()
print "count:\t", df.groupBy ("lname").count().show()
print "select:\t",df.select(df['fname'],df['lname']).show()
result = sqlc.sql("select fname,age from people where age>=35")
print "result:",type(result)
print "select SQL:\t", result.show()
print "Extract from select SQL:\t", result.map(lambda s: "First Name :"+s.fname).collect()
print "basicSum :", basicSum(result.map(lambda s:s.age))
print "Max:\t", df.groupBy("lname").max().show()

#people = sqlc.jsonFile('hdfs://sandbox.hortonworks.com/user/root/data/spark/')

########### my pySpark code ###############
sc.stop()
tnow = datetime.datetime.now()
print("End of Program...Spark : %s" % tnow)
# Make sure to delete the wc directory before uncommeting
# wordcounts.saveAsTextFile("wc")
# print words.toDebugString()
# print bigramcounts.first()
# print bigramcounts.take(10)
# print bigramcounts.keys().take(5)
# print bigramcounts.values().take(5)

