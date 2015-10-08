/*
#-------------------------------------------------------------------------------------- 
#	Weather analysis tasks with Pig, Hive and HBase
#	
# Pig Script for performance comaprision between Pig and Hive
# WeatherPerf.pig - Oct 7th 2015
#	
# Thanks to this following site post
#		http://www.open-bigdata.com/performance-test-pig-vs-hive-code-examples/
# Download the data file from above link 
#		and rename/move it to HDFS /user/root/data/weatherperf.csv
# Run the pig scripts and capture the log (make sure the output directories dont exists)
# pig /media/sf_Data/Hadoop/Scripts/tmp.pig >/media/sf_Data/pig.log 2>&1
#
# Each of the 4 tasks produces seperate directory and stores the results
#	 Examine the log file to not the execution for each of the 
#-------------------------------------------------------------------------------------- 
*/

A = LOAD '/user/root/data/weatherperf.csv' using PigStorage(',');
B = FOREACH A GENERATE $1 as station, (int) SUBSTRING($2,0,4) as year , (int) $4 as tmax, (int) $5 as tmin;
C1 = FILTER B BY tmax > -9999 AND station MATCHES '.*OBSERVATORY.*';
CS1 = FILTER B BY tmax > -9999 AND station MATCHES '.*OBSERVATORY.*';
STORE CS1 into '/user/root/data/input/Weather_Pig4' using PigStorage(',');
D1 = GROUP C1 by (station,year);
E1 = FOREACH D1 GENERATE group.station as station, (int) group.year, MAX(C1.tmax),MIN(C1.tmax),AVG(C1.tmax);
STORE E1 into '/user/root/data/input/Weather_Pig6' using PigStorage(',');

