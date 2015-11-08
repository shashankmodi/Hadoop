# pyspark /media/sf_Data/Hadoop/Scripts/tmpspark.py >/media/sf_Data/Hadoop/Scripts/pyspark.log 2>&1 &
#pyspark /media/sf_Data/Hadoop/Scripts/tmpspark.py >/media/sf_Data/Hadoop/Scripts/pyspark.log 
# 2>/media/sf_Data/Hadoop/Scripts/pyspark.log 1>/media/sf_Data/Hadoop/Scripts/pyspark.log
# export QD=/media_sf_Data/Hadoop/Scripts
# export QS=/media_sf_Data
# pyspark $QS/tmpspark.py >$QD/pyspark.log 2>$QD/pyspark2.log

from pyspark import SparkContext
import datetime

def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

tnow = datetime.datetime.now()
print("Begin of Program...Spark : %s" % tnow)
  
sc =SparkContext(appName="WordCount")
quiet_logs( sc )
# hdfs://sandbox.hortonworks.com
#myLines = sc.textFile('hdfs://sandbox.hortonworks.com/user/root/data/book/warandpeace.txt')

lines = sc.parallelize(['Its fun to have fun,','but you have to know how.']) 
wordcounts = lines.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False) 
print wordcounts.take(5)
print wordcounts.collect()
wordcountsIndex = wordcounts.sortByKey().zipWithIndex()
print wordcountsIndex.collect()

## The following script to count the words in a book....
myLines = sc.textFile('hdfs://sandbox.hortonworks.com/user/root/data/book/warandpeace.txt')
myLines_filtered = myLines.filter( lambda x: len(x) > 0 )
print "Lines Count ....",myLines_filtered.count()

words = myLines_filtered.flatMap(lambda x: x.split())
print "Words Count ....",words.count()
wordcounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
print wordcounts.first()
print wordcounts.take(10)
print wordcounts.keys().take(5)
print wordcounts.values().take(5)
print "Printing... toDebugStrings for all the wordcounts RDDs..."
print wordcounts.toDebugString()
print words.toDebugString()
print myLines.toDebugString()

print "\n\n"
## The following script to count the bigrams in a book....

bigrams= myLines.map(lambda x: x.split(" ")).flatMap(lambda s: [((s[i],s[i+1]),1) for i in range (0 ,len(s)-1)])
print bigrams.take(5)
bigramcounts = bigrams.reduceByKey(lambda x,y:x+y).map(lambda (x,y) : (y,x)).sortByKey(False)
print bigramcounts.first()
print bigramcounts.take(10)
print bigramcounts.keys().take(5)
print bigramcounts.values().take(5)

print "Printing... toDebugStrings for all the bigrams RDDs..."
print bigrams.toDebugString()
print bigramcounts.toDebugString()

print "Using glom method\n\n"
## The following script to count the bigrams in a book....
## glom method which gives accurate results - turns partitions into lists

sentences = myLines.glom()			\
		   .map(lambda x:" ".join(x))	\
		   .flatMap(lambda x: x.split(".")).repartition(4)
bigrams= sentences.map(lambda x: x.split())	\
		.flatMap(lambda s: [((s[i],s[i+1]),1) for i in range (0 ,len(s)-1)])
bigramcounts = bigrams.reduceByKey(lambda x,y:x+y)	\
		      .map(lambda (x,y) : (y,x))	\
		      .sortByKey(False)
print bigramcounts.first()
print bigramcounts.take(10)
print bigramcounts.keys().take(5)
print bigramcounts.values().take(5)

sc.stop()
tnow = datetime.datetime.now()
print("End of Program...Spark : %s" % tnow)
# Make sure to delete the wc directory before uncommeting
# wordcounts.saveAsTextFile("wc")

