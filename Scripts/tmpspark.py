from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,Row
import pyspark.sql.functions as func
import datetime

def quiet_logs( sc ):
  print "Resetting the log level for Spark...",str(sc.version)
  print "Configs :", SparkConf().getAll()
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
  
tnow = datetime.datetime.now()
print("Begin of Program...Spark : %s\n" % tnow)

sc = SparkContext(appName="MaprLab2")
sqlc = SQLContext(sc)
quiet_logs( sc )


#To define indexes:
auctionid = 0
bid = 1
bidtime = 2
bidder = 3
bidderrate = 4
openbid = 5
price = 6
itemtype = 7
daystolive = 8

# https://spark.apache.org/docs/1.3.1/api/python/pyspark.html#subpackages
# Load the file from HDFS
print "\nCreate and analyse the Auction RDD......\n"
auctionRDD = sc.textFile("hdfs://sandbox.hortonworks.com/user/root/data/mapr/auctiondata.csv",10).map(lambda x:x.split(","))

print "Count :",auctionRDD.count()

print "First Record :" ,auctionRDD.first()

print "type take() :", type(auctionRDD.take(5))

for i in auctionRDD.take(5): print i

#print "stats() :",auctionRDD.stats()

print "\ntoDebugString() :",auctionRDD.toDebugString()

print "\nauctionid :",auctionRDD.map(lambda r:r[auctionid]).distinct().count()

print "itemtype :",auctionRDD.map(lambda r:r[itemtype]).distinct().count()

print "\nauctionid :",auctionRDD.map(lambda r:r[auctionid]).distinct().take(5)

print "itemtype :",auctionRDD.map(lambda r:r[itemtype]).distinct().collect()

print "\nBids per item itemtype :",auctionRDD.map(lambda r:(r[itemtype],1)).reduceByKey(lambda x,y:x+y).take(5)

print "Bids per item auctionid :",auctionRDD.map(lambda r:(r[auctionid],1)).reduceByKey(lambda x,y:x+y).take(5)

print "\nMax Bid :",auctionRDD.map(lambda r:r[bid]).reduce(max)
print "Min Bid :",auctionRDD.map(lambda r:r[bid]).reduce(min)
print "Average bids :" ,auctionRDD.count()/ auctionRDD.map(lambda r:r[auctionid]).distinct().count()

print "\nAsc Bid :",auctionRDD.sortBy(lambda r:r[bid],True).take(2)
print "Desc Bid :",auctionRDD.sortBy(lambda r:r[bid],False).take(2)
print "\nFilter :", auctionRDD.filter(lambda r:float(r[price]) >200).count()

print "\nCreate and analyse the Auction Dataframe......\n"

auctionSchemaRDD = auctionRDD.map(lambda a: \
			Row(	auctionid = a[0],	\
				bid = float(a[1]),		\
				bidtime = float(a[2]),		\
				bidder = a[03],		\
				bidderrate = int(a[4]),	\
				openbid = float(a[5]),		\
				price = float(a[6]),		\
				itemtype = a[7],	\
				daystolive = int(a[8])))

auctionDf = sqlc.createDataFrame(auctionSchemaRDD)
auctionDf.registerTempTable("auctions")
							
print "printSchema :\n", auctionDf.printSchema()
print "Head -1 :", auctionDf.head(1)
print "Limit -1 :\n", auctionDf.limit(1).show()
print "toJSON -1 :", auctionDf.toJSON().first()
print "withColumn -1 :", auctionDf.withColumn('bid2', auctionDf.bid + 2).take(2)

print "Record Count :", auctionDf.count()
print "Column :", auctionDf.columns
print "dtypes :", auctionDf.dtypes
print "schema :", auctionDf.schema

print "Describe() :\n", auctionDf.describe().show()
print "explain() :\n", auctionDf.explain(extended=True)

print "\nAction Data Analysis..............."

print "\nauctionid Distinct Count :" , auctionDf.select("auctionid").distinct().count()
print "itemtype Distinct Count :" , auctionDf.select("itemtype").distinct().count()
print "itemtype Distinct Values :" , auctionDf.select("itemtype").distinct().show()
print "\nGroupby :\n" , auctionDf.groupBy("itemtype","auctionid").count().head(10)

print "\nGroupby :" ,auctionDf.groupBy("itemtype","auctionid").count().agg(func.max("count"),func.min("count"),func.avg("count")).show()
print "Groupby :", auctionDf.groupBy("itemtype","auctionid").agg(func.max("bid"),func.min("bid"),func.avg("bid")).show()

print "\nFilter :", auctionDf.filter(auctionDf.price >200).count()
xboxes = sqlc.sql("SELECT itemtype,auctionid,bid,bidtime,bidder,bidderrate,openbid,price FROM auctions WHERE itemtype = 'xbox'")
print "SQL xboxes:\n",xboxes.head(5)
print "SQL xboxes describe:\n",xboxes.describe().show()
sc.stop()

tnow = datetime.datetime.now()
print("\n\nEnd of Program...Spark : %s" % tnow)

'''

# __author__ = 'q756934'
# Begin of Program...Spark : 2015-11-09 19:28:20.568958
# 
# Resetting the log level for Spark... 1.3.1
# Configs : [(u'spark.history.kerberos.keytab', u'none'), (u'spark.yarn.scheduler.heartbeat.interval-ms', u'5000'), (u'spark.history.ui.port', u'18080'), (u'spark.app.name', u'tmpspark.py'), (u'spark.yarn.containerLauncherMaxThreads', u'25'), (u'spark.yarn.queue', u'default'), (u'spark.yarn.applicationMaster.waitTries', u'10'), (u'spark.yarn.preserve.staging.files', u'false'), (u'spark.yarn.driver.memoryOverhead', u'384'), (u'spark.yarn.services', u'org.apache.spark.deploy.yarn.history.YarnHistoryService'), (u'spark.history.provider', u'org.apache.spark.deploy.yarn.history.YarnHistoryProvider'), (u'spark.yarn.submit.file.replication', u'3'), (u'spark.history.kerberos.principal', u'none'), (u'spark.yarn.max.executor.failures', u'3'), (u'spark.yarn.historyServer.address', u'sandbox.hortonworks.com:18080'), (u'spark.driver.extraJavaOptions', u'-Dhdp.version=2.3.0.0-2557'), (u'spark.master', u'local[*]'), (u'spark.yarn.am.extraJavaOptions', u'-Dhdp.version=2.3.0.0-2557'), (u'spark.files', u'file:/media/sf_Data/Hadoop/Scripts/tmpspark.py'), (u'spark.yarn.executor.memoryOverhead', u'384')]
# 
# Create and analyse the Auction RDD......
# 
# Count : 10654
# First Record : [u'8213034705', u'95', u'2.927373', u'jake7870', u'0', u'95', u'117.5', u'xbox', u'3']
# type take() : <type 'list'>
# [u'8213034705', u'95', u'2.927373', u'jake7870', u'0', u'95', u'117.5', u'xbox', u'3']
# [u'8213034705', u'115', u'2.943484', u'davidbresler2', u'1', u'95', u'117.5', u'xbox', u'3']
# [u'8213034705', u'100', u'2.951285', u'gladimacowgirl', u'58', u'95', u'117.5', u'xbox', u'3']
# [u'8213034705', u'117.5', u'2.998947', u'daysrus', u'10', u'95', u'117.5', u'xbox', u'3']
# [u'8213060420', u'2', u'0.065266', u'donnie4814', u'5', u'1', u'120', u'xbox', u'3']
# 
# toDebugString() : (10) PythonRDD[3] at RDD at PythonRDD.scala:43 []
#  |   hdfs://sandbox.hortonworks.com/user/root/data/mapr/auctiondata.csv MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:-2 []
#  |   hdfs://sandbox.hortonworks.com/user/root/data/mapr/auctiondata.csv HadoopRDD[0] at textFile at NativeMethodAccessorImpl.java:-2 []
# 
# auctionid : 627
# itemtype : 3
# 
# auctionid : [u'3021855303', u'3015860830', u'8214435010', u'1648613621', u'1645542737']
# itemtype : [u'palm', u'xbox', u'cartier']
# 
# Bids per item itemtype : [(u'palm', 5917), (u'xbox', 2784), (u'cartier', 1953)]
# Bids per item auctionid : [(u'3021855303', 18), (u'3015860830', 4), (u'8214435010', 35), (u'1648613621', 9), (u'1645542737', 8)]
# 
# Max Bid : 999.99
# Min Bid : 0.01
# Average bids : 16
# 
# Asc Bid : [[u'8212591610', u'0.01', u'0.185104', u'giveglory2', u'8', u'0.01', u'114.5', u'xbox', u'7'], [u'8212668731', u'0.01', u'0.027384', u'elsypineda', u'8', u'0.01', u'167.5', u'xbox', u'7']]
# Desc Bid : [[u'1649718196', u'999.99', u'2.756712963', u'soul.reaver', u'13', u'1', u'1799', u'cartier', u'7'], [u'1643201832', u'999', u'0.300196759', u'artkall', u'54', u'10', u'1599', u'cartier', u'7']]
# 
#  Filter : 7685
# 
# Create and analyse the Auction Dataframe......
# 
# printSchema :
# root
#  |-- auctionid: string (nullable = true)
#  |-- bid: double (nullable = true)
#  |-- bidder: string (nullable = true)
#  |-- bidderrate: long (nullable = true)
#  |-- bidtime: double (nullable = true)
#  |-- daystolive: long (nullable = true)
#  |-- itemtype: string (nullable = true)
#  |-- openbid: double (nullable = true)
#  |-- price: double (nullable = true)
# 
# None
# Head -1 : [Row(auctionid=u'8213034705', bid=95.0, bidder=u'jake7870', bidderrate=0, bidtime=2.9273729999999998, daystolive=3, itemtype=u'xbox', openbid=95.0, price=117.5)]
# Limit -1 :
# auctionid  bid  bidder   bidderrate bidtime  daystolive itemtype openbid price
# 8213034705 95.0 jake7870 0          2.927373 3          xbox     95.0    117.5
# None
# toJSON -1 : {"auctionid":"8213034705","bid":95.0,"bidder":"jake7870","bidderrate":0,"bidtime":2.927373,"daystolive":3,"itemtype":"xbox","openbid":95.0,"price":117.5}
# withColumn -1 : [Row(auctionid=u'8213034705', bid=95.0, bidder=u'jake7870', bidderrate=0, bidtime=2.9273729999999998, daystolive=3, itemtype=u'xbox', openbid=95.0, price=117.5, bid2=97.0), Row(auctionid=u'8213034705', bid=115.0, bidder=u'davidbresler2', bidderrate=1, bidtime=2.9434840000000002, daystolive=3, itemtype=u'xbox', openbid=95.0, price=117.5, bid2=117.0)]
# Record Count : 10654
# Column : [u'auctionid', u'bid', u'bidder', u'bidderrate', u'bidtime', u'daystolive', u'itemtype', u'openbid', u'price']
# dtypes : [('auctionid', 'string'), ('bid', 'double'), ('bidder', 'string'), ('bidderrate', 'bigint'), ('bidtime', 'double'), ('daystolive', 'bigint'), ('itemtype', 'string'), ('openbid', 'double'), ('price', 'double')]
# schema : StructType(List(StructField(auctionid,StringType,true),StructField(bid,DoubleType,true),StructField(bidder,StringType,true),StructField(bidderrate,LongType,true),StructField(bidtime,DoubleType,true),StructField(daystolive,LongType,true),StructField(itemtype,StringType,true),StructField(openbid,DoubleType,true),StructField(price,DoubleType,true)))
# Describe() :
# summary bid                bidderrate         bidtime            daystolive         openbid           price             
# count   10654              10654              10654              10654              10654             10654             
# mean    207.93610756523373 31.98216632250798  3.9752701764003184 5.9384268819222825 52.36264501595644 335.6373728177215 
# stddev  323.3505566150327  120.67323452159712 2.353473672153492  1.5850107447184747 168.642541122904  433.91904067060796
# min     0.01               -4                 5.67E-4            3                  0.01              26.0              
# max     5400.0             3140               6.99999            7                  5000.0            5400.0            
# None
# explain() :
# == Parsed Logical Plan ==
# LogicalRDD [auctionid#0,bid#1,bidder#2,bidderrate#3L,bidtime#4,daystolive#5L,itemtype#6,openbid#7,price#8], MapPartitionsRDD[84] at mapPartitions at SQLContext.scala:1174
# 
# == Analyzed Logical Plan ==
# LogicalRDD [auctionid#0,bid#1,bidder#2,bidderrate#3L,bidtime#4,daystolive#5L,itemtype#6,openbid#7,price#8], MapPartitionsRDD[84] at mapPartitions at SQLContext.scala:1174
# 
# == Optimized Logical Plan ==
# LogicalRDD [auctionid#0,bid#1,bidder#2,bidderrate#3L,bidtime#4,daystolive#5L,itemtype#6,openbid#7,price#8], MapPartitionsRDD[84] at mapPartitions at SQLContext.scala:1174
# 
# == Physical Plan ==
# PhysicalRDD [auctionid#0,bid#1,bidder#2,bidderrate#3L,bidtime#4,daystolive#5L,itemtype#6,openbid#7,price#8], MapPartitionsRDD[84] at mapPartitions at SQLContext.scala:1174
# 
# Code Generation: false
# == RDD ==
# None
# 
# Action Data Analysis...............
# 
# auctionid Distinct Count : 627
# itemtype Distinct Count : 3
# itemtype Distinct Values : itemtype
# xbox    
# palm    
# cartier 
# None
# 
# Groupby : MAX(count) MIN(count) AVG(count)        
# 75         1          16.992025518341308
# None
# Groupby : MAX(bid) MIN(bid) AVG(bid)          
# 207.5    100.0    155.349           
# 120.0    2.0      66.48818181818181 
# 202.49   180.0    191.496           
# 127.5    100.0    114.22222222222223
# 1226.0   35.0     800.6666666666666 
# 217.5    3.0      96.1745           
# 203.5    180.0    192.5             
# 245.0    100.01   178.36999999999998
# 122.5    1.04     42.80057142857143 
# 510.0    305.0    402.22222222222223
# 114.5    1.25     68.06904761904762 
# 227.5    10.0     129.245           
# 2100.0   525.0    1343.4761904761904
# 253.0    20.0     124.15857142857142
# 455.0    50.0     303.04272727272723
# 210.1    185.0    201.02            
# 224.01   200.0    215.59            
# 228.01   80.0     168.7290909090909 
# 96.0     10.0     60.669999999999995
# 224.5    222.0    223.25            
# None
# 
# Filter : 7685
# SQL xboxes:
# [Row(itemtype=u'xbox', auctionid=u'8213034705', bid=95.0, bidtime=2.9273729999999998, bidder=u'jake7870', bidderrate=0, openbid=95.0, price=117.5), Row(itemtype=u'xbox', auctionid=u'8213034705', bid=115.0, bidtime=2.9434840000000002, bidder=u'davidbresler2', bidderrate=1, openbid=95.0, price=117.5), Row(itemtype=u'xbox', auctionid=u'8213034705', bid=100.0, bidtime=2.9512849999999999, bidder=u'gladimacowgirl', bidderrate=58, openbid=95.0, price=117.5), Row(itemtype=u'xbox', auctionid=u'8213034705', bid=117.5, bidtime=2.9989469999999998, bidder=u'daysrus', bidderrate=10, openbid=95.0, price=117.5), Row(itemtype=u'xbox', auctionid=u'8213060420', bid=2.0, bidtime=0.065266000000000005, bidder=u'donnie4814', bidderrate=5, openbid=1.0, price=120.0)]
# SQL xboxes describe:
# summary bid               bidtime            bidderrate         openbid            price             
# count   2784              2784               2784               2784               2784              
# mean    85.39793821839083 4.335952312140802  30.517241379310345 25.669256465517318 144.27594109195385
# stddev  60.31201300728099 2.3929699351933187 131.71545978514823 32.76904272073935  72.93472700540426 
# min     0.01              5.67E-4            -1                 0.01               31.0              
# max     501.77            6.999977           2736               175.0              501.77            
# None
# 
# 
# End of Program...Spark : 2015-11-09 19:30:14.944953
'''