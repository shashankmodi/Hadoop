
 --Load files into relations
    month1 = LOAD 'hdfs:/user/root/data/input/201412hourly.txt' USING PigStorage(',')
    	AS (wban: int,date: chararray,time: chararray,stationtype: int,
            skyCondition: chararray,skyconditionflg: int,visibility: float,visibilityflg: int,weather_type: chararray,weathertypeflg: int,dryTemp: int);;
    month2 = LOAD 'hdfs:/user/root/data/input/201501hourly.txt' USING PigStorage(',')
    	AS (wban: int,date: chararray,time: chararray,stationtype: int,
            skyCondition: chararray,skyconditionflg: int,visibility: float,visibilityflg: int,weather_type: chararray,weathertypeflg: int,dryTemp: int);;
 
    --Combine relations
    months = UNION month1, month2;

    /* Splitting relations
    SPLIT months INTO 
            splitMonth1 IF SUBSTRING(date, 4, 6) == '01',
            splitMonth2 IF SUBSTRING(date, 4, 6) == '02',
            splitMonth3 IF SUBSTRING(date, 4, 6) == '03',
            splitRest IF (SUBSTRING(date, 4, 6) == '04' OR SUBSTRING(date, 4, 6) == '04');
    */

    /*  Joining relations

    stations = LOAD 'hdfs:/data/big/data/QCLCD201211/stations.txt' USING PigStorage() AS (id:int, name:chararray)

    JOIN months BY wban, stations by id;

	http://www.dezyre.com/hadoop-tutorial/hive-tutorial check out some interview questions as well
	https://bluewatersql.wordpress.com/2013/04/17/shakin-bacon-using-pig-to-process-data/
	
	

    */

    --filter out unwanted data
    clearWeather = FILTER months BY skyCondition == 'CLR';

    --Transform and shape relation
    shapedWeather = FOREACH clearWeather GENERATE date, SUBSTRING(date, 0, 4) as year, SUBSTRING(date, 4, 6) as month, SUBSTRING(date, 6, 8) as day, skyCondition, dryTemp;

    --Group relation specifying number of reducers
    groupedByMonthDay = GROUP shapedWeather BY (month, day) PARALLEL 10;
    
    --Aggregate relation
        aggedResults = FOREACH groupedByMonthDay GENERATE group as MonthDay, AVG(shapedWeather.dryTemp), MIN(shapedWeather.dryTemp), MAX(shapedWeather.dryTemp), COUNT(shapedWeather.dryTemp) PARALLEL 10;
    
        --Sort relation
        sortedResults = ORDER aggedResults BY $1 DESC;
    
        --Store results in HDFS
    STORE sortedResults INTO 'hdfs:/user/root/data/weatherpigresults' USING PigStorage(':');