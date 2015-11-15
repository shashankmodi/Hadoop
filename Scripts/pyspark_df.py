# export QD=/media/sf_Data/Hadoop/Scripts
# export QS=/media/sf_Data
# pyspark $QS/tmpspark.py >$QD/pyspark.log 2>$QD/pyspark2.log

from pyspark import SparkContext,SparkConf

import datetime
from pyspark.sql import SQLContext,Row

from pyspark.sql.functions import lit


def quiet_logs( sc ):
  print "Resetting the log level for Spark...",str(sc.version)
  print "Configs :", SparkConf().getAll()
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


# DataFrame methods for Spark 1.3.1 in the following link
#	https://spark.apache.org/docs/1.3.1/api/python/pyspark.sql.html
print "\n\n Dataframe Methods...............................\n"
maxages = df.where (df['age'] >30)
print "printSchema :\n", maxages.printSchema()
print "collect :", maxages.collect()
print "Head -1 :", maxages.head(1)
print "Limit -1 :\n", maxages.limit(1).show()
print "toJSON -1 :", maxages.toJSON().first()
print "withColumn -1 :", maxages.withColumn('age2', df.age + 2).collect()

print "Record Count :", maxages.count()
print "Column :", maxages.columns
print "dtypes :", maxages.dtypes
print "schema :", maxages.schema

print "Describe :", maxages.describe().show()
print "explain :", maxages.explain(extended=True)
#print "toPandas :", maxages.toPandas()  ** does not work

maxagesS = df.groupBy("lname").max().show()
maxagesC = df.groupBy("lname").max().collect()
print "maxagesS Type:",type(maxagesS) ### NoneType
print "maxagesC Type:",type(maxagesC) ### List

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
'''
Begin of Program...Spark : 2015-11-09 19:36:29.898554
Resetting the log level for Spark... 1.3.1
Configs : [(u'spark.history.kerberos.keytab', u'none'), (u'spark.yarn.scheduler.heartbeat.interval-ms', u'5000'), (u'spark.history.ui.port', u'18080'), (u'spark.app.name', u'tmpspark.py'), (u'spark.yarn.containerLauncherMaxThreads', u'25'), (u'spark.yarn.queue', u'default'), (u'spark.yarn.applicationMaster.waitTries', u'10'), (u'spark.yarn.preserve.staging.files', u'false'), (u'spark.yarn.driver.memoryOverhead', u'384'), (u'spark.yarn.services', u'org.apache.spark.deploy.yarn.history.YarnHistoryService'), (u'spark.history.provider', u'org.apache.spark.deploy.yarn.history.YarnHistoryProvider'), (u'spark.yarn.submit.file.replication', u'3'), (u'spark.history.kerberos.principal', u'none'), (u'spark.yarn.max.executor.failures', u'3'), (u'spark.yarn.historyServer.address', u'sandbox.hortonworks.com:18080'), (u'spark.driver.extraJavaOptions', u'-Dhdp.version=2.3.0.0-2557'), (u'spark.master', u'local[*]'), (u'spark.yarn.am.extraJavaOptions', u'-Dhdp.version=2.3.0.0-2557'), (u'spark.files', u'file:/media/sf_Data/Hadoop/Scripts/tmpspark.py'), (u'spark.yarn.executor.memoryOverhead', u'384')]
data_rdd: <class 'pyspark.rdd.RDD'>
df: <class 'pyspark.sql.dataframe.DataFrame'>
RDD (2) ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:392 []
Data Frame root
 |-- fname: string (nullable = true)
 |-- lname: string (nullable = true)
 |-- age: long (nullable = true)

None
fname      lname       age
Udhay      Subramanian 40 
Sundar     Subramanian 35 
Sundar     Subramanian 35 
Mridhini   Udhay       8  
Srishti    Udhay       8  
Anu        Udhay       35 
Saraswathi Palani      38 
None
where:	fname    lname age
Mridhini Udhay 8  
Srishti  Udhay 8  
None
where eq:	fname lname       age
Udhay Subramanian 40 
None
count:	lname       count
Udhay       3    
Palani      1    
Subramanian 3    
None
select:	fname      lname      
Udhay      Subramanian
Sundar     Subramanian
Sundar     Subramanian
Mridhini   Udhay      
Srishti    Udhay      
Anu        Udhay      
Saraswathi Palani     
None
result: <class 'pyspark.sql.dataframe.DataFrame'>
select SQL:	fname      age
Udhay      40 
Sundar     35 
Sundar     35 
Anu        35 
Saraswathi 38 
None
Extract from select SQL:	[u'First Name :Udhay', u'First Name :Sundar', u'First Name :Sundar', u'First Name :Anu', u'First Name :Saraswathi']
basicSum : 183
Max:	lname       MAX(age)
Udhay       35      
Palani      38      
Subramanian 40      
None


 Dataframe Methods...............................

printSchema :
root
 |-- fname: string (nullable = true)
 |-- lname: string (nullable = true)
 |-- age: long (nullable = true)

None
collect : [Row(fname=u'Udhay', lname=u'Subramanian', age=40), Row(fname=u'Sundar', lname=u'Subramanian', age=35), Row(fname=u'Sundar', lname=u'Subramanian', age=35), Row(fname=u'Anu', lname=u'Udhay', age=35), Row(fname=u'Saraswathi', lname=u'Palani', age=38)]
Head -1 : [Row(fname=u'Udhay', lname=u'Subramanian', age=40)]
Limit -1 :
fname lname       age
Udhay Subramanian 40 
None
toJSON -1 : {"fname":"Udhay","lname":"Subramanian","age":40}
withColumn -1 : [Row(fname=u'Udhay', lname=u'Subramanian', age=40, age2=42), Row(fname=u'Sundar', lname=u'Subramanian', age=35, age2=37), Row(fname=u'Sundar', lname=u'Subramanian', age=35, age2=37), Row(fname=u'Anu', lname=u'Udhay', age=35, age2=37), Row(fname=u'Saraswathi', lname=u'Palani', age=38, age2=40)]
Record Count : 5
Column : [u'fname', u'lname', u'age']
dtypes : [('fname', 'string'), ('lname', 'string'), ('age', 'bigint')]
schema : StructType(List(StructField(fname,StringType,true),StructField(lname,StringType,true),StructField(age,LongType,true)))
Describe : summary age              
count   5                
mean    36.6             
stddev  2.059126028197347
min     35               
max     40               
None
explain : == Parsed Logical Plan ==
Filter (age#2L > 30)
 LogicalRDD [fname#0,lname#1,age#2L], MapPartitionsRDD[10] at mapPartitions at SQLContext.scala:1174

== Analyzed Logical Plan ==
Filter (age#2L > CAST(30, LongType))
 LogicalRDD [fname#0,lname#1,age#2L], MapPartitionsRDD[10] at mapPartitions at SQLContext.scala:1174

== Optimized Logical Plan ==
Filter (age#2L > 30)
 LogicalRDD [fname#0,lname#1,age#2L], MapPartitionsRDD[10] at mapPartitions at SQLContext.scala:1174

== Physical Plan ==
Filter (age#2L > 30)
 PhysicalRDD [fname#0,lname#1,age#2L], MapPartitionsRDD[10] at mapPartitions at SQLContext.scala:1174

Code Generation: false
== RDD ==
None
lname       MAX(age)
Udhay       35      
Palani      38      
Subramanian 40      
maxagesS Type: <type 'NoneType'>
maxagesC Type: <type 'list'>
End of Program...Spark : 2015-11-09 19:37:11.281765
'''