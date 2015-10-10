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
# Restart the hadoop services in Hortonworks using cmd if the tasks are failing..
# service startup_script restart
#
# Each of the 4 tasks produces seperate directory and stores the results
#	 Examine the log file to not the execution for each of the 
#-------------------------------------------------------------------------------------- 
*/

/* Task 1 Table ordered by max HighTemperature 
	Table:(Station, Date, MaxTemp)		*/

A = LOAD '/user/root/data/weatherperf.csv' using PigStorage(',');
B = FOREACH A GENERATE $1 as station, (int) SUBSTRING($2,0,4) as year , (int) $4 as tmax, (int) $5 as tmin;
C1 = FILTER B by tmax >-9999; /* C1 = FILTER B by tmax >0;*/
D1 = ORDER C1 BY tmax;
STORE D1 into '/user/root/data/input/Weather_pig1' using PigStorage(',');

/* Task 2 – Max & Average temperatures per year per station - 
	Table:(Distinct:Station, Year, MaxHighTemp,AvgHighTemp)   */

DG1 = GROUP C1 by (station, year);
E1 = FOREACH DG1 GENERATE group.station as station, (int) group.year, MAX(C1.tmax), AVG(C1.tmax);
STORE E1 into '/user/root/data/input/Weather_Pig2' using PigStorage(',');


/* Task 3 – Max mean yearly temperature
	Table:
	(Distinct:Station, MaxYear, MaxMeanTemp)
	 MeanTemp: AVG((MaxTemp-MinTemp)/2) 		*/
	 
D1 = FOREACH C1 GENERATE station as station, year as year, ((tmax-tmin)/2) as avgt;
DESCRIBE D1;
E1 = GROUP D1 by (station, year);
F1 = FOREACH E1 GENERATE group.station as station, (int) group.year, MAX(D1.avgt) as avgt;
G1 = GROUP F1 by station;
H1 = FOREACH G1 {
	I1 = ORDER F1 BY avgt DESC;
	J1 = LIMIT I1 1;
	GENERATE group, J1.(year, avgt);
};
STORE H1 into '/user/root/data/input/Weather_Pig3' using PigStorage(',');

/* Task 4 – Average temperatures per year of station OBSERVATORY
	Select only one weather station and average all time precipitation data.
	Table:(OBSERVATORY, MAX, MIN, Avg HighTemp)	*/
	
C1V = FILTER B BY tmax > -9999 AND station MATCHES '.*OBSERVATORY.*';
D1V = GROUP C1V by station;
E1V = FOREACH D1V GENERATE group as station, MAX(C1V.tmax),MIN(C1V.tmax),AVG(C1V.tmax);
STORE E1V into '/user/root/data/input/Weather_Pig4' using PigStorage(',');


/* Task 5 – Average temperatures per year of station OBSERVATORY
	Select only one weather station and average all time precipitation data.
	Table:(OBSERVATORY, Year, MAX, MIN, Avg HighTemp) includes year for the 2 stations
			*/

A = LOAD '/user/root/data/weatherperf.csv' using PigStorage(',');
B = FOREACH A GENERATE $1 as station, (int) SUBSTRING($2,0,4) as year , (int) $4 as tmax, (int) $5 as tmin;
C1 = FILTER B BY tmax > -9999 AND station MATCHES '.*OBSERVATORY.*';
CS1 = FILTER B BY tmax > -9999 AND station MATCHES '.*OBSERVATORY.*';
STORE CS1 into '/user/root/data/input/Weather_Pig5' using PigStorage(',');
D1 = GROUP C1 by (station,year);
E1 = FOREACH D1 GENERATE group.station as station, (int) group.year, MAX(C1.tmax),MIN(C1.tmax),AVG(C1.tmax);
STORE E1 into '/user/root/data/input/Weather_Pig6' using PigStorage(',');
